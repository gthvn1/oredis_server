(* https://rdb.fnordig.de/file_format.html *)

exception ParseError of string

(** [lenght_encoding ic] returns false and the length if it is not a special
    encoding format. Otherwise it returns true, special format value *)
let lenght_encoding (ic : in_channel) : bool * int =
  let b = input_byte ic in
  match b land 0b11_000000 with
  | 0b00_000000 -> (false, b land 0b00_111111)
  | 0b01_000000 ->
      let high = b land 0b00_111111 in
      let low = input_byte ic in
      (false, (high lsl 8) lor low)
  | 0b10_000000 -> (false, input_byte ic)
  | 0b11_000000 -> (true, b land 0b00_111111)
  | _ -> failwith "Unreachable"

let string_encoding (ic : in_channel) : string =
  (* There are three types of strings in Redis:
      - Length prefixed strings
      - special format: An 8, 16 or 32 bit integer
      - special format: A LZF compressed string
  *)
  let special, v = lenght_encoding ic in
  if special then
    match v with
    | 0 -> input_byte ic |> string_of_int
    | 1 ->
        let high = input_byte ic lsl 8 in
        let low = input_byte ic in
        high lor low |> string_of_int
    | 2 ->
        let n4 = input_byte ic lsl 24 in
        let n3 = input_byte ic lsl 16 in
        let n2 = input_byte ic lsl 8 in
        let n1 = input_byte ic in
        n4 lor n3 lor n2 lor n1 |> string_of_int
    | 3 -> raise (ParseError "Compressed Strings not yet supported")
    | _ -> raise (ParseError "String encoding: unknown special case")
  else really_input_string ic v

let parse_aux (ic : in_channel) : bool =
  let k = string_encoding ic in
  let v = string_encoding ic in
  Printf.printf "AUX: %s/%s\n%!" k v;
  false

let parse_resizedb (ic : in_channel) : bool =
  (* We are expecting two length-encoded int *)
  let special, hash_sz = lenght_encoding ic in
  if special then
    raise (ParseError "special encoding not expected for hash table size");
  let special, exp_hash_sz = lenght_encoding ic in
  if special then
    raise
      (ParseError "special encoding not expected for expire hash table size");
  Printf.printf "HashSZ: %d, Expire HashSZ: %d\n%!" hash_sz exp_hash_sz;
  false

let parse_expiretimems (ic : in_channel) : bool =
  (* It is an 8 bytes values *)
  let sz = 8 in
  let v = Bytes.create sz in
  let _ = really_input ic v (In_channel.pos ic |> Int64.to_int) sz in
  Printf.printf "Expiretimems: Skipping expire time in milliseconds\n%!";
  false

let parse_expiretime (ic : in_channel) : bool =
  (* It is an 4 bytes values *)
  let sz = 4 in
  let v = Bytes.create sz in
  let _ = really_input ic v (In_channel.pos ic |> Int64.to_int) sz in
  Printf.printf "Expiretime: Skipping expire time in seconds\n%!";
  false

let parse_selectdb (ic : in_channel) : bool =
  let b = input_byte ic in
  if b <> 0 then (
    Printf.printf "Found selectdb of %d\n%!" b;
    raise (ParseError "Selectdb: We only support db number 0 for now"));
  false

let parse_eof (ic : in_channel) : bool =
  let _ = ic in
  Printf.printf "TODO: Check CRC\n%!";
  true

let parse_key_value (ty : int) (ic : in_channel) : bool =
  if ty <> 0 then
    raise (ParseError "Only string encoded is supported for key/value");
  let k = string_encoding ic in
  let v = string_encoding ic in
  let resp = Cmd.set_cmd [ k; v ] in
  Printf.printf "Added %s/%s: %s%!" k v resp;
  false

let rec parse_opcodes ic =
  let fini =
    match input_byte ic with
    | 0xFA -> parse_aux ic
    | 0xFB -> parse_resizedb ic
    | 0xFC -> parse_expiretimems ic
    | 0xFD -> parse_expiretime ic
    | 0xFE -> parse_selectdb ic
    | 0xFF -> parse_eof ic
    | ty -> parse_key_value ty ic
  in
  if not fini then parse_opcodes ic

(* Update the memory with Key/Value found in the file *)
let update_mem (ic : in_channel) : unit =
  let buffer = Bytes.create 9 in
  try
    (* Check the header that is 9 bytes *)
    really_input ic buffer 0 9;
    let magic = Bytes.sub_string buffer 0 5 in
    let ver = Bytes.sub_string buffer 5 4 in
    if magic <> "REDIS" then (
      Printf.printf "Found %s instead of REDIS\n%!" magic;
      raise (ParseError "Failed to read the Magic"));
    Printf.printf "Found version 0b%s\n%!" ver;
    (* Now parse opcodes*)
    parse_opcodes ic
  with
  | End_of_file -> raise (ParseError "reach EOF before reading all contents\n")
  | Invalid_argument s -> raise (ParseError s)

let load () : unit =
  let d = Conf.dir () in
  let f = Conf.dbfilename () in
  let path = if String.ends_with ~suffix:"/" d then d ^ f else d ^ "/" ^ f in
  try
    let ic = open_in_bin path in
    (* Read the first byte *)
    update_mem ic;
    close_in ic
  with
  (* Just reports errors and don't do anything *)
  | Sys_error s -> Printf.printf "DB not updated: %s\n%!" s
  | ParseError s -> Printf.printf "Failed to parse DB: %s\n%!" s

let save () = Printf.printf "TODO: save RDB\n%!"
