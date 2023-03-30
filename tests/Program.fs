module Program

open Expecto

[<EntryPoint>]
let main argv =
  runTestsInAssembly
    { defaultConfig with
        runInParallel = false }
    argv
