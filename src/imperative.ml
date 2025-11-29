open! Base
open! Core
open! Core_unix

let name = "imperative"

module Record : sig
  type t = {
    mutable count : int;
    mutable min : float;
    mutable max : float;
    mutable tot : float 
  } [@@deriving sexp]
end
= struct
  type t = {
    mutable count : int;
    mutable min : float;
    mutable max : float;
    mutable tot : float 
  } [@@deriving  sexp]
end

let process_file lines =
  let tbl = String.Table.create () in
  List.iter lines ~f:(fun line ->
    let (town,temp_s) = String.lsplit2_exn line ~on:';' in
    let temp = Float.of_string temp_s in
    let record = Hashtbl.find_or_add tbl town ~default:(fun () -> {Record.count = 0; min = Float.max_value; max = Float.min_value; tot = 0.0}) in
    record.count <- record.count + 1;
    record.min <- Float.min record.min temp;
    record.max <- Float.max record.max temp;
    record.tot <- Float.(record.tot + temp)
  );
  tbl

let compute ~measurements ~outfile =
    let fcontents = In_channel.read_all measurements in
    let lines = String.split_lines fcontents in
    let res = process_file lines in
    let ofd = Out_channel.create outfile in
    let sorted = Hashtbl.to_alist res |> List.sort ~compare:(fun (k1,_) (k2,_) -> String.compare k1 k2) in
    List.iter sorted ~f:(fun (key, data) ->
      let mean = Float.(data.tot / (of_int data.count)) in
      Out_channel.output_string ofd (Printf.sprintf "%s=%.1f/%.1f/%.1f\n" key data.min mean data.max)
    );
