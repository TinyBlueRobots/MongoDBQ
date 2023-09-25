module Program

open Expecto

[<EntryPoint>]
let main args =
  runTestsInAssemblyWithCLIArgs [ Sequenced ] args
