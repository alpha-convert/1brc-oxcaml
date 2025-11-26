(* open! Core
open! Core_unix
open! Parallel
module Parallel_array = Parallel.Arrays.Array

let name = "par"

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
  (* Min, Max, and Tot are all stored as 10x their true value. *)
  type t = {
    town_idx : int;
    town_len : int;
    mutable count : Int64_u.t;
    mutable min : Int64_u.t;
    mutable max : Int64_u.t;
    mutable tot : Int64_u.t
  } [@@deriving sexp]
end
= struct
  type t = {
    town_idx : int;
    town_len : int;
    mutable count : Int64_u.t;
    mutable min : Int64_u.t;
    mutable max : Int64_u.t;
    mutable tot : Int64_u.t
  } [@@deriving sexp]
end

let [@zero_alloc] to_fixed ~fracs ~ones ~tens =
  Int64_u.(#100L * tens + #10L * ones + fracs)

let to_floating fixed =
  Float_u.to_float (Int64_u.to_float fixed) /. 10.0

let process_file (buf @ shared) ~chunk_start ~chunk_end =
  let tbl = Int.Table.create () in
  let buf_len = Bigstring.length buf in

  let dot_code = Char.to_int '.' in
  let dash_code = Char.to_int '-' in

  let [@zero_alloc] parse_temp i =
    let buf_i = Bigstring.unsafe_get_int8 buf ~pos:i in
    let is_neg = Int.(=) buf_i dash_code in
    let i' = Bool.select is_neg (i + 1) i in
    let f = Int64_u.of_int (Bool.select is_neg (-1) 1) in

    let buf0 = Base_bigstring.unsafe_get_int8 buf ~pos:i' in
    let buf1 = Base_bigstring.unsafe_get_int8 buf ~pos:(i' + 1) in
    let buf2 = Base_bigstring.unsafe_get_int8 buf ~pos:(i' + 2) in
    let buf3 = Base_bigstring.unsafe_get_int8 buf ~pos:(i' + 3) in

    let is_dot = Int.(=) buf1 dot_code in

    let fracs = Int64_u.(of_int (Bool.select is_dot buf2 buf3) - #48L) in
    let ones = Int64_u.(of_int (Bool.select is_dot buf0 buf1) - #48L) in
    let tens = Int64_u.(of_int (Bool.select is_dot 0 buf0) - #48L) in

    let v = Int64_u.(f * to_fixed ~fracs ~ones ~tens) in
    #(v, i' + Bool.select is_dot 3 4)
  in

  let [@zero_alloc] rec hash_town_aux h i end_pos =
    if Int.(=) i end_pos then
      h
    else
      hash_town_aux (h * 31 + (Bigstring.unsafe_get_int8 ~pos:i buf)) (i + 1) end_pos
  in
  let [@zero_alloc] hash_town ~start_pos ~end_pos =
    hash_town_aux 0 start_pos end_pos
  in

  let [@zero_alloc] parse_line i =
    let i_semic = unsafe_bigstring_find ~pos:i ~len:(buf_len - i) buf ';' in
    let town_hash = hash_town ~start_pos:i ~end_pos:i_semic in
    let #(temp,i_newline) = parse_temp (i_semic + 1) in
    let town_len = i_semic - i in
    #(town_hash,town_len,temp,i_newline + 1)
  in

  let rec loop i =
    if i >= chunk_end then
      ()
    else
      let #(town_hash,town_len,temp,idx_next) = parse_line i in
      let record = Hashtbl.find_or_add tbl town_hash ~default:(fun () -> {Record.town_idx = i; town_len; count = #0L; min = Int64_u.max_value (); max = Int64_u.min_value (); tot = #0L}) in
      record.count <- Int64_u.(record.count + #1L);
      record.min <- Int64_u.min record.min temp;
      record.max <- Int64_u.max record.max temp;
      record.tot <- Int64_u.(record.tot + temp);
      loop idx_next
  in
  loop chunk_start;
  tbl

let compute ~measurements ~outfile =

  let scheduler = Parallel_scheduler.create ~max_domains:10 () in
  Parallel_scheduler.parallel scheduler ~f:(fun parallel ->
    let meas_fd = openfile ~mode:[O_RDONLY] measurements in
    let buf @ shared = Bigarray.array1_of_genarray (map_file meas_fd Bigarray.char Bigarray.c_layout ~shared:false [|-1|]) in
    let num_chunks = 8 in
    let buf_len = Bigstring.length buf in

    let find_newline_or_end pos =
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
    in

     (* val reduce
    : ('a : value mod contended portable).
    Parallel_kernel.t @ local
    -> 'a t @ shared
    -> f:(Parallel_kernel.t @ local -> 'a -> 'a -> 'a) @ portable
    -> 'a option *)
    let chunk_results =
      Parallel_array.map parallel chunks ~f:(fun _ (chunk_start, chunk_end) ->
        process_file buf ~chunk_start ~chunk_end
      )
    in
    (* let res = Parallel_array.reduce parallel chunks ~f:() *)

    let res = process_file buf ~chunk_start:0 ~chunk_end:buf_len in
    let ofd = Out_channel.create outfile in
    Out_channel.output_string ofd "{";
    Hashtbl.iteri res ~f:(fun ~key:_ ~data ->
      let min = to_floating data.min in
      let max = to_floating data.max in
      let tot = to_floating data.tot in
      let mean = tot /. (Float_u.to_float (Int64_u.to_float data.count)) in
      let town = Bigstring.get_string ~pos:data.town_idx ~len:data.town_len buf in
      Out_channel.output_string ofd (Printf.sprintf "%s=%.1f/%.1f/%.1f," town min mean max)
    );
    Out_channel.output_string ofd "}";
  )



   *)
