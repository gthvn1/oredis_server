(** [is_digit c] returns true if [c] is a digit. *)
let is_digit c = Char.code '0' <= Char.code c && Char.code c <= Char.code '9'

(** [to_digit_exn c] returns the integer value of the character [c] if it is a
    digit. It raises an exception otherwise. *)
let to_digit_exn c =
  if is_digit c then Char.code c - Char.code '0'
  else failwith "Not a valid number"

(** [split_on_crlf str] splits the string [str] on \r\n pattern. It returns the
    list of all strings found. *)
let split_on_crlf (s : string) : string list =
  let l = String.to_seq s |> List.of_seq in
  let rec loop (cl : char list) (acc : char list list) (cur : char list) =
    match cl with
    | [] -> List.rev @@ (List.rev cur :: acc)
    | '\r' :: '\n' :: xs -> loop xs (List.rev cur :: acc) []
    | c :: xs -> loop xs acc (c :: cur)
  in
  loop l [] []
  |> List.map (fun l -> List.to_seq l |> String.of_seq)
  |> List.filter (fun s -> String.length s <> 0)

(** [trim_crlf str] remove all crlf delimiters at the beginning of the string
    [str] if any .*)
let rec trim_crlf (str : string) : string =
  if String.length str < 2 then str
  else if str.[0] = '\r' && str.[1] = '\n' then
    trim_crlf (String.sub str 2 (String.length str - 2))
  else str

(** [print_as_char_lists str] print all characters code of a given string [str].
    It can be use for debugging. *)
let print_as_char_list (str : string) : unit =
  print_string ">>> ";
  str |> String.to_seq |> List.of_seq |> List.map Char.code
  |> List.iter (fun x -> Printf.printf "%d " x)
  |> print_newline

(** [drop_from_start str ~drop] drops the first [drop] characters of the string
    [str]. *)
let drop_from_start (str : string) ~(drop : int) : string =
  let sz = String.length str in
  if drop < sz then String.sub str drop (sz - drop) else ""

(** [get_int_from_start str] returns the integer at the beginning of [str] if
    any and the rest of the string. *)
let get_int_from_start (str : string) : int option * string =
  let cl = String.to_seq str |> List.of_seq in
  let rec loop (l : char list) (acc : int) =
    match l with
    | [] -> (Some acc, "")
    | x :: xs as s ->
        if is_digit x then loop xs ((acc * 10) + to_digit_exn x)
        else
          let rest = List.to_seq s |> String.of_seq in
          (Some acc, rest)
  in
  if is_digit (List.hd cl) then loop (List.tl cl) (to_digit_exn (List.hd cl))
  else (None, str)
