open! Base
open! Core
open! Core_unix
let name = "unboxed"

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

let process_file lines =
  let tbl = String.Table.create () in
  List.iter lines ~f:(fun line ->
    let (town,temp_s) = String.lsplit2_exn line ~on:';' in
    let temp = Float_u.of_string temp_s in
    let record = Hashtbl.find_or_add tbl town ~default:(fun () -> {Record.count = #0L; min = Float_u.max_value (); max = Float_u.min_value (); tot = #0.0}) in
    record.count <- Int64_u.(record.count + #1L);
    record.min <- Float_u.min record.min temp;
    record.max <- Float_u.max record.max temp;
    record.tot <- Float_u.(record.tot + temp)
  );
  tbl

let compute ~measurements ~outfile =
    let fcontents = In_channel.read_all measurements in
    let lines = String.split_lines fcontents in
    let res = process_file lines in
    let ofd = Out_channel.create outfile in
    let sorted = Hashtbl.to_alist res |> List.sort ~compare:(fun (k1,_) (k2,_) -> String.compare k1 k2) in
    List.iter sorted ~f:(fun (key, data) ->
      let mean = Float_u.(data.tot / (Int64_u.to_float data.count)) in
      Out_channel.output_string ofd (Printf.sprintf "%s=%.1f/%.1f/%.1f\n" key (Float_u.to_float data.min) (Float_u.to_float mean) (Float_u.to_float data.max))
    );
