[<AutoOpen>]
module FSharp.Azure.Cosmos.Replace

open Microsoft.Azure.Cosmos

[<Struct>]
type ReplaceOperation<'T> = {
    Item : 'T
    Id : string
    PartitionKey : PartitionKey voption
    RequestOptions : ItemRequestOptions
}

[<Struct>]
type ReplaceConcurrentlyOperation<'T, 'E> = {
    Id : string
    PartitionKey : PartitionKey voption
    RequestOptions : ItemRequestOptions
    Update : 'T -> Async<Result<'T, 'E>>
}

open System

type ReplaceBuilder<'T> (enableContentResponseOnWrite : bool) =
    member _.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            Id = String.Empty
            PartitionKey = ValueNone
            RequestOptions = ItemRequestOptions (EnableContentResponseOnWrite = enableContentResponseOnWrite)
        }
        : ReplaceOperation<'T>

    /// Sets the item being to replace existing with
    [<CustomOperation "item">]
    member _.Item (state : ReplaceOperation<_>, item) = { state with Item = item }

    /// Sets the item being to replace existing with
    [<CustomOperation "id">]
    member _.Id (state : ReplaceOperation<_>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : ReplaceOperation<_>, partitionKey : PartitionKey) = {
        state with
            PartitionKey = ValueSome partitionKey
    }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : ReplaceOperation<_>, partitionKey : string) = {
        state with
            PartitionKey = ValueSome (PartitionKey partitionKey)
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : ReplaceOperation<_>, options : ItemRequestOptions) = {
        state with
            RequestOptions = options
    }

    /// <summary>Sets the eTag to <see cref="PatchItemRequestOptions.IfMatchEtag"/></summary>
    [<CustomOperation "eTag">]
    member _.ETag (state : ReplaceOperation<_>, eTag : string) =
        state.RequestOptions.IfMatchEtag <- eTag
        state

type ReplaceConcurrentlyBuilder<'T, 'E> (enableContentResponseOnWrite : bool) =
    member _.Yield _ =
        {
            Id = String.Empty
            PartitionKey = ValueNone
            RequestOptions = ItemRequestOptions (EnableContentResponseOnWrite = enableContentResponseOnWrite)
            Update =
                fun _ ->
                    raise
                    <| MissingMethodException ("Update function is not set for concurrent replace operation")
        }
        : ReplaceConcurrentlyOperation<'T, 'E>

    /// Sets the item being to replace existing with
    [<CustomOperation "id">]
    member _.Id (state : ReplaceConcurrentlyOperation<_, _>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : ReplaceConcurrentlyOperation<_, _>, partitionKey : PartitionKey) = {
        state with
            PartitionKey = ValueSome partitionKey
    }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : ReplaceConcurrentlyOperation<_, _>, partitionKey : string) = {
        state with
            PartitionKey = ValueSome (PartitionKey partitionKey)
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : ReplaceConcurrentlyOperation<_, _>, options : ItemRequestOptions) =
        options.EnableContentResponseOnWrite <- enableContentResponseOnWrite
        { state with RequestOptions = options }

    /// Sets the partition key
    [<CustomOperation "update">]
    member _.Update (state : ReplaceConcurrentlyOperation<_, _>, update : 'T -> Async<Result<'T, 't>>) = {
        state with
            Update = update
    }

    // ------------------------------------------- Request options -------------------------------------------
    /// <summary>Sets the operation <see cref="ConsistencyLevel"/></summary>
    [<CustomOperation "consistencyLevel">]
    member _.ConsistencyLevel (state : CreateOperation<_>, consistencyLevel : ConsistencyLevel Nullable) =
        state.RequestOptions.ConsistencyLevel <- consistencyLevel

    /// Sets the indexing directive
    [<CustomOperation "indexingDirective">]
    member _.IndexingDirective (state : CreateOperation<_>, indexingDirective : IndexingDirective Nullable) =
        state.RequestOptions.IndexingDirective <- indexingDirective

    /// Adds a trigger to be invoked before the operation
    [<CustomOperation "preTrigger">]
    member _.PreTrigger (state : CreateOperation<_>, trigger : string) =
        state.RequestOptions.PreTriggers <- seq {
            yield! state.RequestOptions.PreTriggers
            yield trigger
        }

    /// Adds triggers to be invoked before the operation
    [<CustomOperation "preTriggers">]
    member _.PreTriggers (state : CreateOperation<_>, triggers : seq<string>) =
        state.RequestOptions.PreTriggers <- seq {
            yield! state.RequestOptions.PreTriggers
            yield! triggers
        }

    /// Adds a trigger to be invoked after the operation
    [<CustomOperation "postTrigger">]
    member _.PostTrigger (state : CreateOperation<_>, trigger : string) =
        state.RequestOptions.PostTriggers <- seq {
            yield! state.RequestOptions.PostTriggers
            yield trigger
        }

    /// Adds triggers to be invoked after the operation
    [<CustomOperation "postTriggers">]
    member _.PostTriggers (state : CreateOperation<_>, triggers : seq<string>) =
        state.RequestOptions.PostTriggers <- seq {
            yield! state.RequestOptions.PostTriggers
            yield! triggers
        }

    /// Sets the session token
    [<CustomOperation "sessionToken">]
    member _.SessionToken (state : CreateOperation<_>, sessionToken : string) =
        state.RequestOptions.SessionToken <- sessionToken

let replace<'T> = ReplaceBuilder<'T> (false)
let replaceAndRead<'T> = ReplaceBuilder<'T> (true)

let replaceConcurrenly<'T, 'E> = ReplaceConcurrentlyBuilder<'T, 'E> (false)
let replaceConcurrenlyAndRead<'T, 'E> = ReplaceConcurrentlyBuilder<'T, 'E> (true)

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

/// Represents the result of a replace operation.
type ReplaceResult<'T> =
    | Ok of 'T // 200
    | BadRequest of ResponseBody : string // 400
    | NotFound of ResponseBody : string // 404
    /// Precondition failed
    | ModifiedBefore of ResponseBody : string // 412 - need re-do
    | EntityTooLarge of ResponseBody : string // 413
    | TooManyRequests of ResponseBody : string * RetryAfter : TimeSpan voption // 429

/// Represents the result of a replace operation.
type ReplaceConcurrentResult<'T, 'E> =
    | Ok of 'T // 200
    | BadRequest of ResponseBody : string // 400
    | NotFound of ResponseBody : string // 404
    /// Precondition failed
    | ModifiedBefore of ResponseBody : string // 412 - need re-do
    | EntityTooLarge of ResponseBody : string // 413
    | TooManyRequests of ResponseBody : string * RetryAfter : TimeSpan voption // 429
    | CustomError of Error : 'E

open System.Net

let private toReplaceResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.BadRequest -> ReplaceResult.BadRequest ex.ResponseBody
    | HttpStatusCode.NotFound -> ReplaceResult.NotFound ex.ResponseBody
    | HttpStatusCode.PreconditionFailed -> ReplaceResult.ModifiedBefore ex.ResponseBody
    | HttpStatusCode.RequestEntityTooLarge -> ReplaceResult.EntityTooLarge ex.ResponseBody
    | HttpStatusCode.TooManyRequests -> ReplaceResult.TooManyRequests (ex.ResponseBody, ex.RetryAfter |> ValueOption.ofNullable)
    | _ -> raise ex

let private toReplaceConcurrentlyErrorResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.NotFound -> ReplaceConcurrentResult.NotFound ex.ResponseBody
    | HttpStatusCode.BadRequest -> ReplaceConcurrentResult.BadRequest ex.ResponseBody
    | HttpStatusCode.PreconditionFailed -> ReplaceConcurrentResult.ModifiedBefore ex.ResponseBody
    | HttpStatusCode.RequestEntityTooLarge -> ReplaceConcurrentResult.EntityTooLarge ex.ResponseBody
    | HttpStatusCode.TooManyRequests ->
        ReplaceConcurrentResult.TooManyRequests (ex.ResponseBody, ex.RetryAfter |> ValueOption.ofNullable)
    | _ -> raise ex

open System.Threading
open System.Threading.Tasks

let rec executeConcurrentlyAsync<'value, 'error>
    (ct : CancellationToken)
    (container : Container)
    (operation : ReplaceConcurrentlyOperation<'value, 'error>)
    (retryAttempts : int)
    : Task<CosmosResponse<ReplaceConcurrentResult<'value, 'error>>> =
    task {
        try
            let partitionKey =
                match operation.PartitionKey with
                | ValueSome partitionKey -> partitionKey
                | ValueNone -> PartitionKey.None

            let! response = container.ReadItemAsync<'value> (operation.Id, partitionKey, cancellationToken = ct)
            let eTag = response.ETag
            let! itemUpdateResult = operation.Update response.Resource

            match itemUpdateResult with
            | Result.Error e -> return CosmosResponse.fromItemResponse (fun _ -> CustomError e) response
            | Result.Ok item ->
                let updateOptions = new ItemRequestOptions (IfMatchEtag = eTag)

                let! response =
                    container.ReplaceItemAsync<'value> (
                        item,
                        operation.Id,
                        requestOptions = updateOptions,
                        cancellationToken = ct
                    )

                return CosmosResponse.fromItemResponse Ok response
        with
        | HandleException ex when
            ex.StatusCode = HttpStatusCode.PreconditionFailed
            && retryAttempts = 1
            ->
            return CosmosResponse.fromException toReplaceConcurrentlyErrorResult ex
        | HandleException ex when ex.StatusCode = HttpStatusCode.PreconditionFailed ->
            return! executeConcurrentlyAsync ct container operation (retryAttempts - 1)
        | HandleException ex -> return CosmosResponse.fromException toReplaceConcurrentlyErrorResult ex
    }

open System.Runtime.InteropServices

[<Literal>]
let DefaultRetryCount = 10

type Microsoft.Azure.Cosmos.Container with

    /// <summary>
    /// Executes a replace operation and returns <see cref="ItemResponse{T}"/>.
    /// </summary>
    /// <param name="operation">Replace operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.PlainExecuteAsync<'T>
        (operation : ReplaceOperation<'T>, [<Optional>] cancellationToken : CancellationToken)
        =
        container.ReplaceItemAsync<'T> (
            operation.Item,
            operation.Id,
            operation.PartitionKey |> ValueOption.toNullable,
            operation.RequestOptions,
            cancellationToken = cancellationToken
        )

    member private container.ExecuteCoreAsync<'T>
        (operation : ReplaceOperation<'T>, [<Optional>] cancellationToken : CancellationToken)
        =
        task {
            try
                let! response = container.PlainExecuteAsync (operation, cancellationToken)
                return CosmosResponse.fromItemResponse ReplaceResult.Ok response
            with HandleException ex ->
                return CosmosResponse.fromException toReplaceResult ex
        }

    /// <summary>
    /// Executes a replace operation safely and returns <see cref="CosmosResponse{ReplaceResult{T}}"/>.
    /// </summary>
    /// <para>
    /// Requires ETag to be set in <see cref="ItemRequestOptions"/>.
    /// </para>
    /// <param name="operation">Replace operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteAsync<'T> (operation : ReplaceOperation<'T>, [<Optional>] cancellationToken : CancellationToken) =
        if String.IsNullOrEmpty operation.RequestOptions.IfMatchEtag then
            invalidArg "eTag" "Safe replace requires ETag"

        container.ExecuteCoreAsync (operation, cancellationToken)

    /// <summary>
    /// Executes a replace operation replacing existing item if it exists and returns <see cref="CosmosResponse{UpsertResult{T}}"/>.
    /// </summary>
    /// <param name="operation">Replace operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteOverwriteAsync<'T> (operation : ReplaceOperation<'T>, [<Optional>] cancellationToken : CancellationToken) =
        container.ExecuteCoreAsync (operation, cancellationToken)

    /// <summary>
    /// Executes a replace operation by applying change to item and returns <see cref="CosmosResponse{ReplaceConcurrentResult{T, E}}"/>.
    /// </summary>
    /// <param name="operation">Replace operation.</param>
    /// <param name="maxRetryCount">Max retry count. Default is 10.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteConcurrentlyAsync<'T, 'E>
        (
            operation : ReplaceConcurrentlyOperation<'T, 'E>,
            [<Optional; DefaultParameterValue(DefaultRetryCount)>] maxRetryCount : int,
            [<Optional>] cancellationToken : CancellationToken
        )
        =
        executeConcurrentlyAsync<'T, 'E> cancellationToken container operation maxRetryCount

    /// <summary>
    /// Executes a replace operation by applying change to item and returns <see cref="CosmosResponse{ReplaceConcurrentResult{T, E}}"/>.
    /// </summary>
    /// <param name="operation">Replace operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteConcurrentlyAsync<'T, 'E>
        (operation : ReplaceConcurrentlyOperation<'T, 'E>, [<Optional>] cancellationToken : CancellationToken)
        =
        executeConcurrentlyAsync<'T, 'E> cancellationToken container operation DefaultRetryCount
