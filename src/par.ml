open! Core
open! Core_unix
open! Parallel
module Par_seq = Parallel.Sequence
module Tbl = Portable_lockfree_htbl

let name = "par"

let magic_shared (x : 'a) : 'a @ shared = Obj.magic Obj.magic x
let magic_uncontended (x : 'a) : 'a @ uncontended = Obj.magic Obj.magic x
let magic_portable (x : 'a) : 'a @ portable = Obj.magic Obj.magic x

(* Bigstring.unsafe_find doesn't yet take its argument shared-ly... *)
external unsafe_bigstring_find
: Bigstring.t @ shared
  -> char
  -> pos:int
  -> len:int
  -> int
  @@ portable
  = "bigstring_find"
[@@noalloc]
    (* let find_newline_or_end pos =
      if pos >= buf_len then buf_len
      else
        let nl_pos = unsafe_bigstring_find ~pos ~len:(buf_len - pos) buf '\n' in
        nl_pos + 1
    in
    let boundaries : int array =
      Array.init (num_chunks + 1) ~f:(fun i ->
        let approx = (i * buf_len) / num_chunks in
          if i = 0 then 0
          else if i = num_chunks then buf_len
          else find_newline_or_end approx)
    in
    let chunks =
      Parallel_array.of_array
        (Array.init num_chunks ~f:(fun i -> (boundaries.(i), boundaries.(i + 1))))
    in *)




module Record : sig
  (* Min, Max, and Tot are all stored as 10x their true value. *)
  type t = {
    town_idx : int;
    town_len : int;
    count : int Atomic.t;
    min : int Atomic.t;
    max : int Atomic.t;
    tot : int Atomic.t
  } [@@deriving sexp]
end
= struct
    type t = {
    town_idx : int;
    town_len : int;
    count : int Atomic.t;
    min : int Atomic.t;
    max : int Atomic.t;
    tot : int Atomic.t
  } [@@deriving sexp]
end

let [@zero_alloc] to_fixed ~fracs ~ones ~tens =
  100 * tens + 10 * ones + fracs

let to_floating fixed =
  (Int.to_float fixed) /. 10.0

let dot_code = Char.to_int '.'
let dash_code = Char.to_int '-'

let [@zero_alloc] parse_temp buf i =
  let buf_i = Bigstring.unsafe_get_int8 buf ~pos:i in
  let is_neg = Int.(=) buf_i dash_code in
  let i' = Bool.select is_neg (i + 1) i in
  let f = Bool.select is_neg (-1) 1 in

  let buf0 = Base_bigstring.unsafe_get_int8 buf ~pos:i' in
  let buf1 = Base_bigstring.unsafe_get_int8 buf ~pos:(i' + 1) in
  let buf2 = Base_bigstring.unsafe_get_int8 buf ~pos:(i' + 2) in
  let buf3 = Base_bigstring.unsafe_get_int8 buf ~pos:(i' + 3) in

  let is_dot = Int.(=) buf1 dot_code in

  let fracs = Bool.select is_dot buf2 buf3 - 48 in
  let ones = Bool.select is_dot buf0 buf1 - 48 in
  let tens = Bool.select is_dot 0 (buf0 - 48) in

  f * to_fixed ~fracs ~ones ~tens

let [@zero_alloc] rec hash_town_aux buf h i end_pos =
  if Int.(=) i end_pos then
    h
  else
    hash_town_aux buf (h * 31 + (Bigstring.unsafe_get_int8 ~pos:i buf)) (i + 1) end_pos

let [@zero_alloc] hash_town buf ~start_pos ~end_pos =
  hash_town_aux buf 0 start_pos end_pos

let [@zero_alloc] parse_line buf ~(start_idx : int) ~(semic_idx : int) =
  let town_hash = hash_town buf ~start_pos:start_idx ~end_pos:semic_idx in
  let temp = parse_temp buf (semic_idx + 1) in
  #(~town_hash,~temp)


let split_entries (buf @ shared) : (start_idx:int * semic_idx:int) Par_seq.t =
  let next _ (start_idx,end_idx) = 
    if start_idx >= end_idx then
      Pair_or_null.none ()
    else
      let semic_idx = unsafe_bigstring_find ~pos:start_idx ~len:(end_idx - start_idx) buf ';' in
      let newline_idx = unsafe_bigstring_find ~pos:semic_idx ~len:(end_idx - semic_idx) buf '\n' in
      Pair_or_null.some (~start_idx,~semic_idx) (newline_idx + 1,end_idx)
  in
  let split _ (start_idx,end_idx) =
    if start_idx >= end_idx then
      Pair_or_null.none ()
    else
      if unsafe_bigstring_find ~pos:start_idx ~len:(end_idx - start_idx) buf '\n' < 0 then
        Pair_or_null.none ()
      else
        let half_approx = (start_idx + end_idx) / 2 in
        let half_start = 1 + unsafe_bigstring_find ~pos:half_approx ~len:(end_idx - half_approx) buf '\n' in
        Pair_or_null.some (start_idx,half_start) (half_start,end_idx)
  in
  let buf_len : int = Bigstring.length buf in
  exclave_ (Par_seq.unfold ~init:((0,buf_len) : int * int) ~next:(magic_portable next) ~split:(magic_portable split))

let compute_record  (buf @ shared) (tbl @ contended) ~start_idx ~semic_idx =
  let #(~town_hash,~temp) = parse_line buf ~start_idx ~semic_idx in
  let maybe_record = Tbl.find tbl town_hash in
  match%optional.Or_null (magic_uncontended maybe_record) with
  | None ->
      ignore (Tbl.add tbl ~key:town_hash ~data:{Record.town_idx = start_idx;
               town_len = semic_idx - start_idx;
               count = Atomic.make 1;
               min = Atomic.make temp;
               max = Atomic.make temp;
               tot = Atomic.make temp})
  | Some record -> 
    Atomic.incr record.count;
    Atomic.set record.min (Int.min temp (Atomic.get record.min));
    Atomic.set record.max (Int.max temp (Atomic.get record.max));
    Atomic.add record.tot temp


module Int_hashable : Tbl.Hashable with type t = Int.t = struct
  type t = Int.t
  let equal x y = Int.(=) x y
  let hash x = Int.hash x
end

let compute ~measurements ~outfile =
  let scheduler = Parallel_scheduler.create ~max_domains:1 () in
  Parallel_scheduler.parallel scheduler ~f:(fun parallel ->
    let meas_fd = openfile ~mode:[O_RDONLY] measurements in
    let buf @ shared = Bigarray.array1_of_genarray (map_file meas_fd Bigarray.char Bigarray.c_layout ~shared:false [|-1|]) in
    let tbl = Tbl.create (module Int_hashable) ~min_buckets:10000 in

    let entries = split_entries buf in
    Par_seq.iter parallel entries ~f:(fun _ (~start_idx,~semic_idx) -> compute_record (magic_shared buf) tbl ~start_idx ~semic_idx);

    let ofd = Out_channel.create outfile in
    Out_channel.output_string ofd "{";
    List.iter (magic_uncontended (Tbl.to_alist tbl)) ~f:(fun (_,data)->
      let min = to_floating (Atomic.get data.min) in
      let max = to_floating (Atomic.get data.max) in
      let tot = to_floating (Atomic.get data.tot) in
      let mean = tot /. (Int.to_float (Atomic.get data.count)) in
      let town = Bigstring.get_string ~pos:data.town_idx ~len:data.town_len (Obj.magic Obj.magic buf) in
      Out_channel.output_string ofd (Printf.sprintf "%s=%.1f/%.1f/%.1f," town min mean max)
    );
    Out_channel.output_string ofd "}";
  ) 


