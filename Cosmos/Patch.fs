[<AutoOpen>]
module FSharp.Azure.Cosmos.Patch

open System.Collections.Immutable
open System.Linq
open Microsoft.Azure.Cosmos

[<Struct>]
type PatchOperation<'a> = {
    Operations : PatchOperation list
    Id : string
    PartitionKey : PartitionKey
    RequestOptions : PatchItemRequestOptions voption
}

open System

type PatchBuilder<'a> () =
    member _.Yield _ =
        {
            Operations = []
            Id = String.Empty
            PartitionKey = PartitionKey.None
            RequestOptions = ValueNone
        }
        : PatchOperation<'a>

    /// Adds the <see href"PatchOp">Patch operation</see>
    [<CustomOperation "operation">]
    member _.Operation (state : inref<PatchOperation<'a>>, operation) = { state with Operations = operation :: state.Operations }

    /// Sets the item being to Patch existing with
    [<CustomOperation "id">]
    member _.Id (state : inref<PatchOperation<'a>>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : inref<PatchOperation<'a>>, partitionKey : PartitionKey) = {
        state with
            PartitionKey = partitionKey
    }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : inref<PatchOperation<'a>>, partitionKey : string) = {
        state with
            PartitionKey = (PartitionKey partitionKey)
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : inref<PatchOperation<'a>>, options : PatchItemRequestOptions) = {
        state with
            RequestOptions = ValueSome options
    }

    /// Sets the eTag to <see href="IfMatchEtag">IfMatchEtag</see>
    [<CustomOperation "eTagValue">]
    member _.ETagValue (state : inref<PatchOperation<'a>>, eTag : string) =
        match state.RequestOptions with
        | ValueSome options ->
            options.IfMatchEtag <- eTag
            state
        | ValueNone ->
            let options = PatchItemRequestOptions (IfMatchEtag = eTag)
            { state with RequestOptions = ValueSome options }

    /// Enable content response on write
    member private _.EnableContentResponseOnWrite (state : inref<PatchOperation<'a>>, enable) =
        match state.RequestOptions with
        | ValueSome options ->
            options.EnableContentResponseOnWrite <- enable
            state
        | ValueNone ->
            let options = PatchItemRequestOptions (EnableContentResponseOnWrite = enable)
            { state with RequestOptions = ValueSome options }

    /// Enables content response on write
    [<CustomOperation "enableContentResponseOnWrite">]
    member this.EnableContentResponseOnWrite (state : inref<PatchOperation<'a>>) =
        this.EnableContentResponseOnWrite (&state, true)

    /// Disanables content response on write
    [<CustomOperation "disableContentResponseOnWrite">]
    member this.DisableContentResponseOnWrite (state : inref<PatchOperation<'a>>) =
        this.EnableContentResponseOnWrite (&state, false)

let patch<'a> = PatchBuilder<'a> ()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type PatchResult<'t> =
    | Ok of 't // 200
    | NotExecuted
    | BadRequest of ResponseBody : string // 400
    | NotFound of ResponseBody : string // 404
    | ModifiedBefore of ResponseBody : string //412 - need re-do
    | TooManyRequests of ResponseBody : string * RetryAfter : TimeSpan voption // 429

open System.Net

let private toPatchResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.BadRequest -> PatchResult.BadRequest ex.ResponseBody
    | HttpStatusCode.NotFound -> PatchResult.NotFound ex.ResponseBody
    | HttpStatusCode.PreconditionFailed -> PatchResult.ModifiedBefore ex.ResponseBody
    | HttpStatusCode.TooManyRequests -> PatchResult.TooManyRequests (ex.ResponseBody, ex.RetryAfter |> ValueOption.ofNullable)
    | _ -> raise ex

open System.Threading
open System.Threading.Tasks

open System.Runtime.InteropServices

type Microsoft.Azure.Cosmos.Container with

    member private container.ExecuteCoreAsync<'a>
        (operation : PatchOperation<'a>, [<Optional>] cancellationToken : CancellationToken)
        =
        task {
            try
                let! response =
                    container.PatchItemAsync<'a> (
                        operation.Id,
                        operation.PartitionKey,
                        operation.Operations.ToImmutableList (),
                        operation.RequestOptions |> ValueOption.toObj,
                        cancellationToken = cancellationToken
                    )
                return CosmosResponse.fromItemResponse PatchResult.Ok response
            with HandleException ex ->
                return CosmosResponse.fromException toPatchResult ex
        }

    member container.ExecuteAsync<'a>
        (operation : inref<PatchOperation<'a>>, [<Optional>] cancellationToken : CancellationToken)
        =
        match operation.RequestOptions with
        | ValueNone -> invalidArg "eTag" "Safe Patch requires ETag"
        | ValueSome options when String.IsNullOrEmpty options.IfMatchEtag -> invalidArg "eTag" "Safe Patch requires ETag"
        | _ -> ()
        container.ExecuteCoreAsync<'a> (operation, cancellationToken)

    member container.ExecuteOverwriteAsync<'a>
        (operation : inref<PatchOperation<'a>>, [<Optional>] cancellationToken : CancellationToken)
        =
        container.ExecuteCoreAsync<'a> (operation, cancellationToken)
