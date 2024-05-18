[<AutoOpen>]
module FSharp.Azure.Cosmos.Patch

open System.Collections.Immutable
open System.Linq
open Microsoft.Azure.Cosmos

[<Struct>]
type PatchOperation<'T> = {
    Operations : PatchOperation list
    Id : string
    PartitionKey : PartitionKey
    RequestOptions : PatchItemRequestOptions
}

open System

type PatchBuilder<'T> (enableContentResponseOnWrite : bool) =
    member _.Yield _ =
        {
            Operations = []
            Id = String.Empty
            PartitionKey = PartitionKey.None
            RequestOptions = PatchItemRequestOptions (EnableContentResponseOnWrite = enableContentResponseOnWrite)
        }
        : PatchOperation<'T>

    /// Adds the <see href"PatchOp">Patch operation</see>
    [<CustomOperation "operation">]
    member _.Operation (state : PatchOperation<'T>, operation) = { state with Operations = operation :: state.Operations }

    /// Sets the item being to Patch existing with
    [<CustomOperation "id">]
    member _.Id (state : PatchOperation<'T>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : PatchOperation<'T>, partitionKey : PartitionKey) = { state with PartitionKey = partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : PatchOperation<'T>, partitionKey : string) = {
        state with
            PartitionKey = (PartitionKey partitionKey)
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : PatchOperation<'T>, options : PatchItemRequestOptions) =
        options.EnableContentResponseOnWrite <- state.RequestOptions.EnableContentResponseOnWrite
        { state with RequestOptions = options }

    /// Sets the eTag to <see href="IfMatchEtag">IfMatchEtag</see>
    [<CustomOperation "eTag">]
    member _.ETag (state : PatchOperation<'T>, eTag : string) =
        state.RequestOptions.IfMatchEtag <- eTag
        state

let patch<'T> = PatchBuilder<'T> (false)
let patchWithContentResponse<'T> = PatchBuilder<'T> (true)

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

/// Represents the result of a patch operation.
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

    /// <summary>
    /// Executes a patch operation and returns <see cref="ItemResponse{T}"/>.
    /// </summary>
    /// <param name="operation">Patch operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.PlainExecuteAsync<'T>
        (operation : PatchOperation<'T>, [<Optional>] cancellationToken : CancellationToken)
        =
        container.PatchItemAsync<'T> (
            operation.Id,
            operation.PartitionKey,
            operation.Operations.ToImmutableList (),
            operation.RequestOptions,
            cancellationToken = cancellationToken
        )

    member private container.ExecuteCoreAsync<'T>
        (operation : PatchOperation<'T>, [<Optional>] cancellationToken : CancellationToken)
        =
        task {
            try
                let! response = container.PlainExecuteAsync<'T> (operation, cancellationToken)
                return CosmosResponse.fromItemResponse PatchResult.Ok response
            with HandleException ex ->
                return CosmosResponse.fromException toPatchResult ex
        }

    /// <summary>
    /// Executes a patch operation safely and returns <see cref="CosmosResponse{PatchResult{T}}"/>.
    /// <para>
    /// Requires ETag to be set in <see cref="PatchItemRequestOptions"/>.
    /// </summary>
    /// <param name="operation">Patch operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteAsync<'T> (operation : PatchOperation<'T>, [<Optional>] cancellationToken : CancellationToken) =
        if String.IsNullOrEmpty operation.RequestOptions.IfMatchEtag then
            invalidArg "eTag" "Safe patch requires ETag"

        container.ExecuteCoreAsync<'T> (operation, cancellationToken)

    /// <summary>
    /// Executes a patch operation and returns <see cref="CosmosResponse{PatchResult{T}}"/>.
    /// </summary>
    /// <param name="operation">Patch operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteOverwriteAsync<'T>
        (operation : PatchOperation<'T>, [<Optional>] cancellationToken : CancellationToken)
        =
        container.ExecuteCoreAsync<'T> (operation, cancellationToken)
