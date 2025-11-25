open! Core
open! Core_unix
let name = "fixed_precision"

module Record : sig
  (* Min, Max, and Tot are all stored as 10x their true value. *)
  type t = {
    mutable count : Int64_u.t;
    mutable min : Int64_u.t;
    mutable max : Int64_u.t;
    mutable tot : Int64_u.t
  } [@@deriving sexp]
end
= struct
  type t = {
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

let process_file buf =
  let tbl = String.Table.create () in
  let buf_len = Bigstring.length buf in

  let [@zero_alloc] digit_at i = Int64_u.of_int (Char.get_digit_exn (Bigstring.unsafe_get buf i)) in
  let [@zero_alloc] parse_positive (i : int) =
    if Char.(=) (Bigstring.unsafe_get buf (i + 1)) '.' then
      (* x.y *)
      let ones = digit_at i in
      let fracs = digit_at (i + 2) in
      #(to_fixed ~fracs ~ones ~tens:#0L, i + 3)
    else
      (* xy.z *)
      let tens = digit_at i in
      let ones = digit_at (i + 1) in
      let fracs = digit_at (i + 3) in
      #(to_fixed ~fracs ~ones ~tens ,i + 4)
  in

  let [@zero_alloc] parse_temp i =
    let maybe_negative = Bigstring.unsafe_get buf i in
    if Char.(maybe_negative = '-') then
      let #(v,end_idx) = parse_positive (i + 1) in
      #(Int64_u.(-#1L * v),end_idx)
    else
      parse_positive i
    in

  let parse_line i = 
    let i_semic = Bigstring.unsafe_find ~pos:i ~len:(buf_len - i) buf ';' in
    let town = Bigstring.get_string ~pos:i ~len:(i_semic - i) buf in
    let #(temp,i_newline) = parse_temp (i_semic + 1) in
    #(town,temp,i_newline + 1) in

  let rec loop i =
    if i >= buf_len then () else
      let #(town,temp,idx_next) = parse_line i in
      let record = Hashtbl.find_or_add tbl town ~default:(fun () -> {Record.count = #0L; min = Int64_u.max_value (); max = Int64_u.min_value (); tot = #0L}) in
      record.count <- Int64_u.(record.count + #1L);
      record.min <- Int64_u.min record.min temp;
      record.max <- Int64_u.max record.max temp;
      record.tot <- Int64_u.(record.tot + temp);
      loop idx_next
  in
  loop 0;
  tbl

let compute ~measurements ~outfile =
  let meas_fd = openfile ~mode:[O_RDONLY] measurements in
  let meas_data : Bigstring.t = Bigarray.array1_of_genarray (map_file meas_fd Bigarray.char Bigarray.c_layout ~shared:false [|-1|]) in
  let res = process_file meas_data in
  let ofd = Out_channel.create outfile in
  Out_channel.output_string ofd "{";
  Hashtbl.iteri res ~f:(fun ~key ~data ->
    let min = to_floating data.min in
    let max = to_floating data.max in
    let tot = to_floating data.tot in
    let mean = tot /. (Float_u.to_float (Int64_u.to_float data.count)) in
    Out_channel.output_string ofd (Printf.sprintf "%s=%.1f/%.1f/%.1f," key min mean max)
  );
  Out_channel.output_string ofd "}";
