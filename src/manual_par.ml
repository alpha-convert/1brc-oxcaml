open! Core
open! Core_unix
open! Parallel
module Par_seq = Parallel.Sequence

let name = "manual_par"

let [@zero_alloc assume] magic_shared (x : 'a) : 'a @ shared = Obj.magic Obj.magic x

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
    mutable count : int;
    mutable min : int;
    mutable max : int;
    mutable tot : int  } [@@deriving sexp]
end
= struct
    type t = {
    town_idx : int;
    town_len : int;
    mutable count : int;
    mutable min : int;
    mutable max : int;
    mutable tot : int  }
     [@@deriving sexp]
end

let [@zero_alloc] [@inline] to_fixed ~fracs ~ones ~tens =
  100 * tens + 10 * ones + fracs

let to_floating fixed =
  (Int.to_float fixed) /. 10.0


let [@zero_alloc] [@inline] parse_temp buf i =
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

  let newline_idx = i + (Bool.select is_neg 1 0) + (Bool.select is_short 3 4) in
  #(~temp:(sign * to_fixed ~fracs:frac ~ones ~tens),~newline_idx)

let semic_broadcast = 0x3B3B3B3B3B3B3B3BL
let lo_magic = 0x0101010101010101L
let hi_magic = 0x8080808080808080L

(* Polynomial hash for 8 bytes: h * 31^8 + b0*31^7 + b1*31^6 + ... + b7 *)
let [@zero_alloc] [@inline] hash_word h word =
  let open Int64_u in
  let word = of_int64 word in
  let h = h * #31L + (word land #0xFFL) in
  let h = h * #31L + ((lsr) word 8 land #0xFFL) in
  let h = h * #31L + ((lsr) word 16 land #0xFFL) in
  let h = h * #31L + ((lsr) word 24 land #0xFFL) in
  let h = h * #31L + ((lsr) word 32 land #0xFFL) in
  let h = h * #31L + ((lsr) word 40 land #0xFFL) in
  let h = h * #31L + ((lsr) word 48 land #0xFFL) in
  h * #31L + ((lsr) word 56 land #0xFFL)

let [@zero_alloc] [@inline] [@loop] rec hash_town_slow buf h i =
  let b = Bigstring.unsafe_get_int8 ~pos:i buf in
  if Int.(=) b 59 then
    #(~town_hash:(Int64_u.to_int_trunc h),~semic_idx:i)
  else
    hash_town_slow buf Int64_u.(h * #31L + of_int b) (i + 1)

let [@zero_alloc] [@inline] [@loop] rec hash_town_aux buf (h : Int64_u.t) i =
  let word = Base_bigstring.unsafe_get_int64_t_le buf ~pos:i in
  let xored = Int64.(word lxor semic_broadcast) in
  let has_semic = Int64.((xored - lo_magic) land (lnot xored) land hi_magic) in
  if Int64.(has_semic <> 0L) then
    hash_town_slow buf h i
  else
    hash_town_aux buf (hash_word h word) (i + 8)

let [@zero_alloc] [@inline] hash_town buf ~start_pos =
  hash_town_aux buf #0L start_pos

let [@zero_alloc] [@inline] parse_line buf ~(start_idx : int) =
  let #(~town_hash,~semic_idx) = hash_town buf ~start_pos:start_idx in
  let #(~temp,~newline_idx) = parse_temp buf (semic_idx + 1) in
  #(~town_hash,~temp,~semic_idx,~newline_idx)

let compute_record (buf @ shared) tbl ~start_idx =
  let #(~town_hash,~temp,~semic_idx,~newline_idx) = parse_line buf ~start_idx in
  let record = Hashtbl.find_or_add tbl town_hash ~default:(fun () ->
    {Record.town_idx = start_idx; town_len = semic_idx - start_idx; count = 0; min = Int.max_value ; max = Int.min_value; tot = 0})
  in
  record.count <- record.count + 1;
  record.min <- Int.min record.min temp;
  record.max <- Int.max record.max temp;
  record.tot <- record.count + temp;
  newline_idx


let compute ~measurements ~outfile =
  let meas_fd = openfile ~mode:[O_RDONLY] measurements in
  let buf @ shared = Bigarray.array1_of_genarray (map_file meas_fd Bigarray.char Bigarray.c_layout ~shared:false [|-1|]) in

  let buf_len = Bigstring.length buf in
  let num_chunks = 100 in
  let chunk_size = buf_len / num_chunks in

  let chunk_bounds =
    List.init num_chunks ~f:(fun i ->
      let start_pos =
        if i = 0 then 0
        else
          let approx = i * chunk_size in
          1 + unsafe_bigstring_find (magic_shared buf) '\n' ~pos:approx ~len:(buf_len - approx)
      in
      let end_pos =
        if i = num_chunks - 1 then buf_len
        else
          let approx = (i + 1) * chunk_size in
          1 + unsafe_bigstring_find (magic_shared buf) '\n' ~pos:approx ~len:(buf_len - approx)
      in
      (start_pos, end_pos))
  in

  let tasks =
    let rec go i end_idx tbl = 
      if i >= end_idx then
        tbl
      else
        let newline_idx = compute_record (magic_shared buf) tbl ~start_idx:i in
        go (newline_idx + 1) end_idx tbl
    in
    List.map chunk_bounds ~f:(fun (start_idx,end_idx) -> Domain.spawn (fun () -> let tbl = Int.Table.create () in go start_idx end_idx tbl))
  in

  let results = List.map tasks ~f:Domain.join in

  (* Merge all per-domain tables into one *)
  let merged = Int.Table.create () in
  List.iter results ~f:(fun tbl ->
    Hashtbl.iteri tbl ~f:(fun ~key ~data ->
      Hashtbl.update merged key ~f:(function
        | None -> data
        | Some existing ->
          existing.count <- existing.count + data.count;
          existing.min <- Int.min existing.min data.min;
          existing.max <- Int.max existing.max data.max;
          existing.tot <- existing.tot + data.tot;
          existing)));

  let ofd = Out_channel.create outfile in
  let with_towns = Hashtbl.to_alist merged |> List.map ~f:(fun (_, data) ->
    let town = Bigstring.get_string ~pos:data.town_idx ~len:data.town_len (Obj.magic Obj.magic buf) in
    (town, data)) in
  let sorted = List.sort with_towns ~compare:(fun (k1,_) (k2,_) -> String.compare k1 k2) in
  List.iter sorted ~f:(fun (town, data) ->
    let min = to_floating data.min in
    let max = to_floating data.max in
    let tot = to_floating data.tot in
    let mean = tot /. (Int.to_float data.count) in
    Out_channel.output_string ofd (Printf.sprintf "%s=%.1f/%.1f/%.1f\n" town min mean max)
  );


