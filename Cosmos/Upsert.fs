[<AutoOpen>]
module FSharp.Azure.Cosmos.Upsert

open Microsoft.Azure.Cosmos

[<Struct>]
type UpsertOperation<'T> = {
    Item : 'T
    PartitionKey : PartitionKey voption
    RequestOptions : ItemRequestOptions
}

[<Struct>]
type UpsertConcurrentlyOperation<'T, 'E> = {
    Id : string
    PartitionKey : PartitionKey voption
    RequestOptions : ItemRequestOptions
    UpdateOrCreate : 'T option -> Async<Result<'T, 'E>>
}

open System

type UpsertBuilder<'T> (enableContentResponseOnWrite : bool) =
    member _.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            PartitionKey = ValueNone
            RequestOptions = ItemRequestOptions (EnableContentResponseOnWrite = enableContentResponseOnWrite)
        }
        : UpsertOperation<'T>

    /// Sets the item being creeated
    [<CustomOperation "item">]
    member _.Item (state : UpsertOperation<_>, item) = { state with Item = item }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : UpsertOperation<_>, partitionKey : PartitionKey) = {
        state with
            PartitionKey = ValueSome partitionKey
    }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : UpsertOperation<_>, partitionKey : string) = {
        state with
            PartitionKey = ValueSome (PartitionKey partitionKey)
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : UpsertOperation<_>, options : ItemRequestOptions) =
        options.EnableContentResponseOnWrite <- enableContentResponseOnWrite
        { state with RequestOptions = options }

    /// <summary>Sets the eTag to <see cref="PatchItemRequestOptions.IfMatchEtag"/></summary>
    [<CustomOperation "eTag">]
    member _.ETag (state : UpsertOperation<_>, eTag : string) =
        state.RequestOptions.IfMatchEtag <- eTag; state

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
        state.RequestOptions.PreTriggers <- seq {
            yield! state.RequestOptions.PreTriggers
            yield trigger
        }
        state

    /// Adds triggers to be invoked before the operation
    [<CustomOperation "preTriggers">]
    member _.PreTriggers (state : CreateOperation<_>, triggers : seq<string>) =
        state.RequestOptions.PreTriggers <- seq {
            yield! state.RequestOptions.PreTriggers
            yield! triggers
        }
        state

    /// Adds a trigger to be invoked after the operation
    [<CustomOperation "postTrigger">]
    member _.PostTrigger (state : CreateOperation<_>, trigger : string) =
        state.RequestOptions.PostTriggers <- seq {
            yield! state.RequestOptions.PostTriggers
            yield trigger
        }
        state

    /// Adds triggers to be invoked after the operation
    [<CustomOperation "postTriggers">]
    member _.PostTriggers (state : CreateOperation<_>, triggers : seq<string>) =
        state.RequestOptions.PostTriggers <- seq {
            yield! state.RequestOptions.PostTriggers
            yield! triggers
        }
        state

    /// Sets the session token
    [<CustomOperation "sessionToken">]
    member _.SessionToken (state : CreateOperation<_>, sessionToken : string) =
        state.RequestOptions.SessionToken <- sessionToken; state

type UpsertConcurrentlyBuilder<'T, 'E> (enableContentResponseOnWrite : bool) =
    member _.Yield _ =
        {
            Id = String.Empty
            PartitionKey = ValueNone
            RequestOptions = ItemRequestOptions (EnableContentResponseOnWrite = enableContentResponseOnWrite)
            UpdateOrCreate =
                function
                | _ ->
                    raise
                    <| MissingMethodException ("Update function is not set for concurrent upsert operation")
        }
        : UpsertConcurrentlyOperation<'T, 'E>

    /// Sets the item being to upsert existing with
    [<CustomOperation "id">]
    member _.Id (state : UpsertConcurrentlyOperation<_, _>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : UpsertConcurrentlyOperation<_, _>, partitionKey : PartitionKey) = {
        state with
            PartitionKey = ValueSome partitionKey
    }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : UpsertConcurrentlyOperation<_, _>, partitionKey : string) = {
        state with
            PartitionKey = ValueSome (PartitionKey partitionKey)
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : UpsertConcurrentlyOperation<_, _>, options : ItemRequestOptions) =
        options.EnableContentResponseOnWrite <- enableContentResponseOnWrite
        { state with RequestOptions = options }

    /// Sets the partition key
    [<CustomOperation "updateOrCreate">]
    member _.UpdateOrCreate (state : UpsertConcurrentlyOperation<_, _>, update : 'T option -> Async<Result<'T, 't>>) = {
        state with
            UpdateOrCreate = update
    }

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
        state.RequestOptions.PreTriggers <- seq {
            yield! state.RequestOptions.PreTriggers
            yield trigger
        }
        state

    /// Adds triggers to be invoked before the operation
    [<CustomOperation "preTriggers">]
    member _.PreTriggers (state : CreateOperation<_>, triggers : seq<string>) =
        state.RequestOptions.PreTriggers <- seq {
            yield! state.RequestOptions.PreTriggers
            yield! triggers
        }
        state

    /// Adds a trigger to be invoked after the operation
    [<CustomOperation "postTrigger">]
    member _.PostTrigger (state : CreateOperation<_>, trigger : string) =
        state.RequestOptions.PostTriggers <- seq {
            yield! state.RequestOptions.PostTriggers
            yield trigger
        }
        state

    /// Adds triggers to be invoked after the operation
    [<CustomOperation "postTriggers">]
    member _.PostTriggers (state : CreateOperation<_>, triggers : seq<string>) =
        state.RequestOptions.PostTriggers <- seq {
            yield! state.RequestOptions.PostTriggers
            yield! triggers
        }
        state

    /// Sets the session token
    [<CustomOperation "sessionToken">]
    member _.SessionToken (state : CreateOperation<_>, sessionToken : string) =
        state.RequestOptions.SessionToken <- sessionToken; state

let upsert<'T> = UpsertBuilder<'T> (false)
let upsertAndRead<'T> = UpsertBuilder<'T> (true)

let upsertConcurrenly<'T, 'E> = UpsertConcurrentlyBuilder<'T, 'E> (false)
let upsertConcurrenlyAndRead<'T, 'E> = UpsertConcurrentlyBuilder<'T, 'E> (true)

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

/// Represents the result of an upsert operation.
type UpsertResult<'t> =
    | Ok of 't // 200
    | BadRequest of ResponseBody : string // 400
    | ModifiedBefore of ResponseBody : string // 412 - need re-do
    | EntityTooLarge of ResponseBody : string // 413
    | TooManyRequests of ResponseBody : string * RetryAfter : TimeSpan voption // 429

/// Represents the result of an upsert operation.
type UpsertConcurrentResult<'t, 'E> =
    | Ok of 't // 200
    | BadRequest of ResponseBody : string // 400
    | ModifiedBefore of ResponseBody : string //412 - need re-do
    | EntityTooLarge of ResponseBody : string // 413
    | TooManyRequests of ResponseBody : string * RetryAfter : TimeSpan voption // 429
    | CustomError of Error : 'E

open System.Net

let private toUpsertResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.BadRequest -> UpsertResult.BadRequest ex.ResponseBody
    | HttpStatusCode.PreconditionFailed -> UpsertResult.ModifiedBefore ex.ResponseBody
    | HttpStatusCode.RequestEntityTooLarge -> UpsertResult.EntityTooLarge ex.ResponseBody
    | _ -> raise ex

let private toUpsertConcurrentlyErrorResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.BadRequest -> UpsertConcurrentResult.BadRequest ex.ResponseBody
    | HttpStatusCode.PreconditionFailed -> UpsertConcurrentResult.ModifiedBefore ex.ResponseBody
    | HttpStatusCode.RequestEntityTooLarge -> UpsertConcurrentResult.EntityTooLarge ex.ResponseBody
    | HttpStatusCode.TooManyRequests ->
        UpsertConcurrentResult.TooManyRequests (ex.ResponseBody, ex.RetryAfter |> ValueOption.ofNullable)
    | _ -> raise ex

open System.Threading
open System.Threading.Tasks

let rec executeConcurrentlyAsync<'value, 'error>
    (ct : CancellationToken)
    (container : Container)
    (operation : UpsertConcurrentlyOperation<'value, 'error>)
    (retryAttempts : int)
    : Task<CosmosResponse<UpsertConcurrentResult<'value, 'error>>> =
    task {

        let! itemResult, response = task {
            let partitionKey =
                match operation.PartitionKey with
                | ValueSome partitionKey -> partitionKey
                | ValueNone -> PartitionKey.None

            try
                let! response = container.ReadItemAsync<'value> (operation.Id, partitionKey, cancellationToken = ct)
                let! itemResult = operation.UpdateOrCreate (Some response.Resource)
                return itemResult, Choice1Of2 response
            with HandleException ex when ex.StatusCode = HttpStatusCode.NotFound ->
                let! itemResult = operation.UpdateOrCreate None
                return itemResult, Choice2Of2 ex
        }

        try
            match itemResult, response with
            | Result.Error e, Choice1Of2 response -> return CosmosResponse.fromItemResponse (fun _ -> CustomError e) response
            | Result.Error e, Choice2Of2 ex -> return CosmosResponse.fromException (fun _ -> CustomError e) ex
            | Result.Ok item, Choice1Of2 response ->
                let updateOptions = ItemRequestOptions (IfMatchEtag = response.ETag)

                let! response =
                    container.UpsertItemAsync<'value> (
                        item,
                        operation.PartitionKey |> ValueOption.toNullable,
                        requestOptions = updateOptions,
                        cancellationToken = ct
                    )

                return CosmosResponse.fromItemResponse Ok response
            | Result.Ok item, Choice2Of2 ex ->
                let! response =
                    container.UpsertItemAsync<'value> (
                        item,
                        operation.PartitionKey |> ValueOption.toNullable,
                        cancellationToken = ct
                    )

                return CosmosResponse.fromItemResponse Ok response
        with
        | HandleException ex when
            ex.StatusCode = HttpStatusCode.PreconditionFailed
            && retryAttempts = 1
            ->
            return CosmosResponse.fromException toUpsertConcurrentlyErrorResult ex
        | HandleException ex when ex.StatusCode = HttpStatusCode.PreconditionFailed ->
            return! executeConcurrentlyAsync ct container operation (retryAttempts - 1)
        | HandleException ex -> return CosmosResponse.fromException toUpsertConcurrentlyErrorResult ex
    }

open System.Runtime.InteropServices

[<Literal>]
let DefaultMaxRetryCount = 10

type Microsoft.Azure.Cosmos.Container with

    /// <summary>
    /// Executes an upsert operation and returns <see cref="ItemResponse{T}"/>.
    /// </summary>
    /// <param name="operation">Upsert operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.PlainExecuteAsync<'T>
        (operation : UpsertOperation<'T>, [<Optional>] cancellationToken : CancellationToken)
        =
        container.UpsertItemAsync<'T> (
            operation.Item,
            operation.PartitionKey |> ValueOption.toNullable,
            operation.RequestOptions,
            cancellationToken = cancellationToken
        )

    member private container.ExecuteCoreAsync<'T>
        (operation : UpsertOperation<'T>, [<Optional>] cancellationToken : CancellationToken)
        =
        task {
            try
                let! response = container.PlainExecuteAsync (operation, cancellationToken)
                return CosmosResponse.fromItemResponse UpsertResult.Ok response
            with HandleException ex ->
                return CosmosResponse.fromException toUpsertResult ex
        }

    /// <summary>
    /// Executes an upsert operation safely and returns <see cref="CosmosResponse{UpsertResult{T}}"/>.
    /// <para>
    /// Requires ETag to be set in <see cref="ItemRequestOptions"/>.
    /// </para>
    /// </summary>
    /// <param name="operation">Upsert operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteAsync<'T> (operation : UpsertOperation<'T>, [<Optional>] cancellationToken : CancellationToken) =
        if String.IsNullOrEmpty operation.RequestOptions.IfMatchEtag then
            invalidArg "eTag" "Safe replace requires ETag"

        container.ExecuteCoreAsync (operation, cancellationToken)

    /// <summary>
    /// Executes an upsert operation replacing existing item if it exists and returns <see cref="CosmosResponse{UpsertResult{T}}"/>.
    /// </summary>
    /// <param name="operation">Upsert operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteOverwriteAsync<'T>
        (operation : UpsertOperation<'T>, [<Optional>] cancellationToken : CancellationToken)
        =
        container.ExecuteCoreAsync (operation, cancellationToken)

    /// <summary>
    /// Executes an upsert operation by applying change to item if exists and returns <see cref="CosmosResponse{UpsertConcurrentResult{T, E}}"/>.
    /// </summary>
    /// <param name="operation">Upsert operation.</param>
    /// <param name="maxRetryCount">Max retry count. Default is 10.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteConcurrentlyAsync<'T, 'E>
        (
            operation : UpsertConcurrentlyOperation<'T, 'E>,
            [<Optional; DefaultParameterValue(DefaultMaxRetryCount)>] maxRetryCount : int,
            [<Optional>] cancellationToken : CancellationToken
        )
        =
        executeConcurrentlyAsync<'T, 'E> cancellationToken container operation maxRetryCount

    /// <summary>
    /// Executes an upsert operation by applying change to item if exists and returns <see cref="CosmosResponse{UpsertConcurrentResult{T, E}}"/>.
    /// </summary>
    /// <param name="operation">Upsert operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteConcurrentlyAsync<'T, 'E>
        (operation : UpsertConcurrentlyOperation<'T, 'E>, [<Optional>] cancellationToken : CancellationToken)
        =
        executeConcurrentlyAsync<'T, 'E> cancellationToken container operation DefaultMaxRetryCount
