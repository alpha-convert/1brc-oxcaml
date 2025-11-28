open! Core
open! Core_unix
open! Parallel
module Par_seq = Parallel.Sequence
module Tbl = Portable_lockfree_htbl
let name = "swar_temp"

let magic_shared (x : 'a) : 'a @ shared = Obj.magic Obj.magic x
let magic_uncontended (x : 'a) : 'a @ uncontended = Obj.magic Obj.magic x

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


module Record : sig
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


let [@zero_alloc] parse_temp buf i =
  let word = Base_bigstring.unsafe_get_int64_t_le buf ~pos:i in

  let byte0 = Int64.(word land 0xFFL) in
  let is_neg = Int64.(=) byte0 0x2DL (* '-' *) in
  let shift = Bool.select is_neg 8 0 in
  let sign = Bool.select is_neg (-1) 1 in

  (* Option 1: x.y_
     Option 2: xx.y
  *)
  let nums_word = Int64.(lsr) word shift in
  let b0 = Int64.(to_int_trunc (nums_word land 0xFFL)) in
  let b1 = Int64.(to_int_trunc ((lsr) nums_word 8 land 0xFFL)) in
  let b2 = Int64.(to_int_trunc ((lsr) nums_word 16 land 0xFFL)) in
  let b3 = Int64.(to_int_trunc ((lsr) nums_word 24 land 0xFFL)) in

  let is_short = Int.(=) b1 0x2E (* '.' *) in

  let tens = Bool.select is_short 0 (b0 - 48) in
  let ones = Bool.select is_short (b0 - 48) (b1 - 48) in
  let frac = Bool.select is_short (b2 - 48) (b3 - 48) in

  sign * to_fixed ~fracs:frac ~ones ~tens

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

let max_with temp (x @ contended) @ portable = Int.max temp x
let min_with temp (x @ contended) @ portable = Int.min temp x

let update_record (record : Record.t) temp =
  Atomic.incr record.count;
  Atomic.update record.min ~pure_f:(min_with temp);
  Atomic.update record.max ~pure_f:(max_with temp);
  Atomic.add record.tot temp

let compute_record (buf @ shared) tbl ~start_idx ~semic_idx =
  let #(~town_hash,~temp) = parse_line buf ~start_idx ~semic_idx in
  let maybe_record = Tbl.find tbl town_hash in
  match%optional.Or_null (magic_uncontended maybe_record) with
  | None ->
      let data = {Record.town_idx = start_idx; town_len = semic_idx - start_idx; count = Atomic.make 1; min = Atomic.make temp; max = Atomic.make temp; tot = Atomic.make temp} in
      (match%optional.Or_null (Tbl.add tbl ~key:town_hash ~data) with
      | None -> ()
      | Some record -> update_record record temp)
  | Some record -> update_record record temp

module Int_hashable : Tbl.Hashable with type t = Int.t = struct
  type t = Int.t
  let equal x y = Int.(=) x y
  let hash x = Int.hash x
end

let [@zero_alloc] pack a b = (a lsl 31) lor b
let [@zero_alloc] unpack_start_idx st = st lsr 31
let [@zero_alloc] unpack_end_idx st = st land 0x7FFFFFFF


let compute ~measurements ~outfile =
  let scheduler = Parallel_scheduler.create ~max_domains:100 () in
  Parallel_scheduler.parallel scheduler ~f:(fun parallel ->
    let meas_fd = openfile ~mode:[O_RDONLY] measurements in
    let buf @ shared = Bigarray.array1_of_genarray (map_file meas_fd Bigarray.char Bigarray.c_layout ~shared:false [|-1|]) in
    let tbl = Tbl.create (module Int_hashable) ~min_buckets:10000 in

    let fork _ st =
      let start_idx = unpack_start_idx st in
      let end_idx = unpack_end_idx st in
      if start_idx >= end_idx then
        Pair_or_null.none ()
      else
        if unsafe_bigstring_find ~pos:start_idx ~len:(end_idx - start_idx) (magic_shared buf) '\n' < 0 then
          Pair_or_null.none ()
        else
          let half_approx = (start_idx + end_idx) / 2 in
          let half_start = 1 + unsafe_bigstring_find ~pos:half_approx ~len:(end_idx - half_approx) (magic_shared buf) '\n' in
          Pair_or_null.some (pack start_idx half_start) (pack half_start end_idx)
    in
    let next _ () st  =
      let start_idx = unpack_start_idx st in
      let end_idx = unpack_end_idx st in
      if start_idx >= end_idx then
        Pair_or_null.none ()
      else
        let semic_idx = unsafe_bigstring_find ~pos:start_idx ~len:(end_idx - start_idx) (magic_shared buf) ';' in
        let newline_idx = unsafe_bigstring_find ~pos:semic_idx ~len:(end_idx - semic_idx) (magic_shared buf) '\n' in
        compute_record (magic_shared buf) tbl ~start_idx ~semic_idx;
        Pair_or_null.some () (pack (newline_idx + 1) end_idx)
    in

    Parallel.fold
      parallel
      ~init:(fun () -> ())
      ~state:((pack 0 (Bigstring.length buf)))
      ~stop:(fun _ () -> ())
      ~join:(fun _ () () -> ())
      ~next
      ~fork;

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


