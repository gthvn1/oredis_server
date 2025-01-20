type t = { dir : string; dbfilename : string }

let rconf_mutex = Mutex.create ()
let rconf = ref { dir = ""; dbfilename = "" }

let dir () =
  Mutex.lock rconf_mutex;
  let conf = !rconf in
  Mutex.unlock rconf_mutex;
  conf.dir

let set_dir (dir : string) =
  Mutex.lock rconf_mutex;
  rconf := { !rconf with dir };
  Mutex.unlock rconf_mutex

let dbfilename () =
  Mutex.lock rconf_mutex;
  let conf = !rconf in
  Mutex.unlock rconf_mutex;
  conf.dbfilename

let set_dbfilename (dbfilename : string) =
  Mutex.lock rconf_mutex;
  rconf := { !rconf with dbfilename };
  Mutex.unlock rconf_mutex
