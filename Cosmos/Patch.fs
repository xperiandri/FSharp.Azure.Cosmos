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
    RequestOptions : PatchItemRequestOptions
}

open System

type PatchBuilder<'a> (enableContentResponseOnWrite : bool) =
    member _.Yield _ =
        {
            Operations = []
            Id = String.Empty
            PartitionKey = PartitionKey.None
            RequestOptions = PatchItemRequestOptions (EnableContentResponseOnWrite = enableContentResponseOnWrite)
        }
        : PatchOperation<'a>

    /// Adds the <see href"PatchOp">Patch operation</see>
    [<CustomOperation "operation">]
    member _.Operation (state : PatchOperation<'a>, operation) = { state with Operations = operation :: state.Operations }

    /// Sets the item being to Patch existing with
    [<CustomOperation "id">]
    member _.Id (state : PatchOperation<'a>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : PatchOperation<'a>, partitionKey : PartitionKey) = { state with PartitionKey = partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : PatchOperation<'a>, partitionKey : string) = {
        state with
            PartitionKey = (PartitionKey partitionKey)
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : PatchOperation<'a>, options : PatchItemRequestOptions) =
        options.EnableContentResponseOnWrite <- state.RequestOptions.EnableContentResponseOnWrite
        { state with RequestOptions = options }

    /// Sets the eTag to <see href="IfMatchEtag">IfMatchEtag</see>
    [<CustomOperation "eTag">]
    member _.ETag (state : PatchOperation<'a>, eTag : string) =
        state.RequestOptions.IfMatchEtag <- eTag
        state

let patch<'a> = PatchBuilder<'a> (false)
let patchWithContentResponse<'a> = PatchBuilder<'a> (true)

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

open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks

type Microsoft.Azure.Cosmos.Container with

    member container.PlainExecuteAsync<'a>
        (operation : PatchOperation<'a>, [<Optional>] cancellationToken : CancellationToken)
        =
        container.PatchItemAsync<'a> (
            operation.Id,
            operation.PartitionKey,
            operation.Operations.ToImmutableList (),
            operation.RequestOptions,
            cancellationToken = cancellationToken
        )

    member private container.ExecuteCoreAsync<'a>
        (operation : PatchOperation<'a>, [<Optional>] cancellationToken : CancellationToken)
        =
        task {
            try
                let! response = container.PlainExecuteAsync<'a> (operation, cancellationToken)
                return CosmosResponse.fromItemResponse PatchResult.Ok response
            with HandleException ex ->
                return CosmosResponse.fromException toPatchResult ex
        }

    member container.ExecuteAsync<'a> (operation : PatchOperation<'a>, [<Optional>] cancellationToken : CancellationToken) =
        if String.IsNullOrEmpty operation.RequestOptions.IfMatchEtag then
            invalidArg "eTag" "Safe patch requires ETag"

        container.ExecuteCoreAsync<'a> (operation, cancellationToken)

    member container.ExecuteOverwriteAsync<'a>
        (operation : PatchOperation<'a>, [<Optional>] cancellationToken : CancellationToken)
        =
        container.ExecuteCoreAsync<'a> (operation, cancellationToken)
