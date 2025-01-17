(* For the first implementation of SET/GET we are using in-memory storage. *)
type 'a t = { store : (string, 'a) Hashtbl.t; mutex : Mutex.t }

let create () : 'a t = { store = Hashtbl.create 1024; mutex = Mutex.create () }

let set (db : 'a t) ~(key : string) ~(value : 'a) : unit =
  Mutex.lock db.mutex;
  Hashtbl.replace db.store key value;
  Mutex.unlock db.mutex

let get (db : 'a t) ~(key : string) : 'a option =
  Mutex.lock db.mutex;
  let result = Hashtbl.find_opt db.store key in
  Mutex.unlock db.mutex;
  result
