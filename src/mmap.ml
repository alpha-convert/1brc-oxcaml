open! Core
open! Core_unix
let name = "mmap"

module Record : sig
  type t = {
    mutable count : Int64_u.t;
    mutable min : Float_u.t;
    mutable max : Float_u.t;
    mutable tot : Float_u.t
  } [@@deriving sexp]
end
= struct
  type t = {
    mutable count : Int64_u.t;
    mutable min : Float_u.t;
    mutable max : Float_u.t;
    mutable tot : Float_u.t
  } [@@deriving sexp]
end

let process_file buf =
  let tbl = String.Table.create () in
  let buf_len = Bigstring.length buf in
  let parse_from i = 
    let i_semic = Bigstring.unsafe_find ~pos:i ~len:(buf_len - i) buf ';' in
    let i_newline = Bigstring.unsafe_find ~pos:i_semic ~len:(buf_len - i_semic) buf '\n' in
    let town = Bigstring.get_string ~pos:i ~len:(i_semic - i) buf in
    let temp_s = Bigstring.get_string ~pos:(i_semic + 1) ~len:(i_newline - (i_semic + 1)) buf in
    #(town,temp_s,i_newline + 1)
  in
  let rec loop i =
    if i >= buf_len  then () else
      let #(town,temp_s,idx_next) = parse_from i in
      let temp = Float_u.of_string temp_s in
      let record = Hashtbl.find_or_add tbl town ~default:(fun () -> {Record.count = #0L; min = Float_u.max_value (); max = Float_u.min_value (); tot = #0.0}) in
      record.count <- Int64_u.(record.count + #1L);
      record.min <- Float_u.min record.min temp;
      record.max <- Float_u.max record.max temp;
      record.tot <- Float_u.(record.tot + temp);
      loop idx_next
  in
  loop 0;
  tbl

let compute ~measurements ~outfile =
  let meas_fd = openfile ~mode:[O_RDONLY] measurements in
  let meas_data : Bigstring.t = Bigarray.array1_of_genarray (map_file meas_fd Bigarray.char Bigarray.c_layout ~shared:false [|-1|]) in
  let res = process_file meas_data in
  let ofd = Out_channel.create outfile in
  let sorted = Hashtbl.to_alist res |> List.sort ~compare:(fun (k1,_) (k2,_) -> String.compare k1 k2) in
  List.iter sorted ~f:(fun (key, data) ->
    let mean = Float_u.(data.tot / (Int64_u.to_float data.count)) in
    Out_channel.output_string ofd (Printf.sprintf "%s=%.1f/%.1f/%.1f\n" key (Float_u.to_float data.min) (Float_u.to_float mean) (Float_u.to_float data.max))
  );
