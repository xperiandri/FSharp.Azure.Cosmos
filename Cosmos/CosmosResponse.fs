namespace FSharp.Azure.Cosmos

open System
open System.Net
open Microsoft.Azure.Cosmos

type CosmosResponse<'t> =
    { HttpStatusCode : HttpStatusCode
      Headers : Headers
      Result : 't
      Diagnostics : CosmosDiagnostics }
    with
        member this.RequestCharge = match this.Headers with null -> 0. | headers -> headers.RequestCharge
        member this.ActivityId = match this.Headers with null -> String.Empty  | headers -> headers.ActivityId
        member this.ETag = match this.Headers with null -> String.Empty | headers -> headers.ETag

module CosmosResponse =

    let fromItemResponse (successFn : 't -> 'r) (response : ItemResponse<'t>) =
        { HttpStatusCode = response.StatusCode
          Headers = response.Headers
          Result = successFn response.Resource
          Diagnostics = response.Diagnostics }

    let fromException resultFn (ex : CosmosException) =
        { HttpStatusCode = ex.StatusCode
          Headers = ex.Headers
          Result = resultFn ex
          Diagnostics = ex.Diagnostics }
