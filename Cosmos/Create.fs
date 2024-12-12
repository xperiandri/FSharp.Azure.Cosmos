[<AutoOpen>]
module FSharp.Azure.Cosmos.Create

open System
open Microsoft.Azure.Cosmos

[<Struct>]
type CreateOperation<'T> = {
    Item : 'T
    PartitionKey : PartitionKey voption
    RequestOptions : ItemRequestOptions
}

type CreateBuilder<'T> (enableContentResponseOnWrite : bool) =
    member _.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            PartitionKey = ValueNone
            RequestOptions = ItemRequestOptions (EnableContentResponseOnWrite = enableContentResponseOnWrite)
        }
        : CreateOperation<'T>

    /// Sets the item being created
    [<CustomOperation "item">]
    member _.Item (state : CreateOperation<_>, item) = { state with Item = item }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : CreateOperation<_>, partitionKey : PartitionKey) = {
        state with
            PartitionKey = ValueSome partitionKey
    }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : CreateOperation<_>, partitionKey : string) = {
        state with
            PartitionKey = ValueSome (PartitionKey partitionKey)
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : CreateOperation<_>, options : ItemRequestOptions) =
        options.EnableContentResponseOnWrite <- enableContentResponseOnWrite
        { state with RequestOptions = options }

    // ------------------------------------------- Request options -------------------------------------------
    /// <summary>Sets the operation <see cref="ConsistencyLevel"/></summary>
    [<CustomOperation "consistencyLevel">]
    member _.ConsistencyLevel (state : CreateOperation<_>, consistencyLevel : ConsistencyLevel Nullable) =
        state.RequestOptions.ConsistencyLevel <- consistencyLevel; state

    /// Sets the indexing directive
    [<CustomOperation "indexingDirective">]
    member _.IndexingDirective (state : CreateOperation<_>, indexingDirective : IndexingDirective Nullable) =
        state.RequestOptions.IndexingDirective <- indexingDirective; state

    /// Adds a trigger to be invoked before the operation
    [<CustomOperation "preTrigger">]
    member _.PreTrigger (state : CreateOperation<_>, trigger : string) =
        state.RequestOptions.AddPreTrigger trigger; state

    /// Adds triggers to be invoked before the operation
    [<CustomOperation "preTriggers">]
    member _.PreTriggers (state : CreateOperation<_>, triggers : seq<string>) =
        state.RequestOptions.AddPreTriggers triggers; state

    /// Adds a trigger to be invoked after the operation
    [<CustomOperation "postTrigger">]
    member _.PostTrigger (state : CreateOperation<_>, trigger : string) =
        state.RequestOptions.AddPostTrigger trigger; state

    /// Adds triggers to be invoked after the operation
    [<CustomOperation "postTriggers">]
    member _.PostTriggers (state : CreateOperation<_>, triggers : seq<string>) =
        state.RequestOptions.AddPostTriggers triggers; state

    /// Sets the session token
    [<CustomOperation "sessionToken">]
    member _.SessionToken (state : CreateOperation<_>, sessionToken : string) =
        state.RequestOptions.SessionToken <- sessionToken; state

let create<'T> = CreateBuilder<'T> (false)
let createAndRead<'T> = CreateBuilder<'T> (true)

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

/// Represents the result of a create operation.
type CreateResult<'T> =
    | Ok of 'T // 201
    | BadRequest of ResponseBody : string // 400
    /// Forbidden
    | PartitionStorageLimitReached of ResponseBody : string // 403
    /// Conflict
    | IdAlreadyExists of ResponseBody : string // 409
    | EntityTooLarge of ResponseBody : string // 413
    | TooManyRequests of ResponseBody : string * RetryAfter : TimeSpan voption // 429

open System.Net
let private toCreateResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.BadRequest -> CreateResult.BadRequest ex.ResponseBody
    | HttpStatusCode.Forbidden -> CreateResult.PartitionStorageLimitReached ex.ResponseBody
    | HttpStatusCode.Conflict -> CreateResult.IdAlreadyExists ex.ResponseBody
    | HttpStatusCode.RequestEntityTooLarge -> CreateResult.EntityTooLarge ex.ResponseBody
    | HttpStatusCode.TooManyRequests -> CreateResult.TooManyRequests (ex.ResponseBody, ex.RetryAfter |> ValueOption.ofNullable)
    | _ -> raise ex

open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks

type Microsoft.Azure.Cosmos.Container with

    /// <summary>
    /// Executes a create operation and returns <see cref="ItemResponse{T}"/>.
    /// </summary>
    /// <param name="operation">Create operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.PlainExecuteAsync<'T> (operation : CreateOperation<'T>, [<Optional>] cancellationToken : CancellationToken) =
        container.CreateItemAsync<'T> (
            operation.Item,
            operation.PartitionKey |> ValueOption.toNullable,
            operation.RequestOptions,
            cancellationToken = cancellationToken
        )

    /// <summary>
    /// Executes a create operation and returns <see cref="CosmosResponse{CreateResult{T}}"/>.
    /// </summary>
    /// <param name="operation">Create operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteAsync<'T>
        (operation : CreateOperation<'T>, [<Optional>] cancellationToken : CancellationToken)
        : Task<CosmosResponse<CreateResult<'T>>>
        =
        task {
            try
                let! response = container.PlainExecuteAsync (operation, cancellationToken)
                return CosmosResponse.fromItemResponse CreateResult.Ok response
            with HandleException ex ->
                return CosmosResponse.fromException toCreateResult ex
        }
