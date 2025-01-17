type t = { dir : string; dbfilename : string }

let create ~dir ~dbfilename : t = { dir; dbfilename }
let dir (conf : t) = conf.dir
let dbfilename (conf : t) = conf.dbfilename
