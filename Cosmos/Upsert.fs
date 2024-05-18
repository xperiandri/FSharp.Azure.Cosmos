[<AutoOpen>]
module FSharp.Azure.Cosmos.Upsert

open Microsoft.Azure.Cosmos

[<Struct>]
type UpsertOperation<'a> = {
    Item : 'a
    PartitionKey : PartitionKey voption
    RequestOptions : ItemRequestOptions
}

[<Struct>]
type UpsertConcurrentlyOperation<'a, 'e> = {
    Id : string
    PartitionKey : PartitionKey voption
    RequestOptions : ItemRequestOptions
    UpdateOrCreate : 'a option -> Async<Result<'a, 'e>>
}

open System

type UpsertBuilder<'a> (enableContentResponseOnWrite : bool) =
    member _.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            PartitionKey = ValueNone
            RequestOptions = ItemRequestOptions (EnableContentResponseOnWrite = enableContentResponseOnWrite)
        }
        : UpsertOperation<'a>

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

    /// Sets the eTag to <see href="IfMatchEtag">IfMatchEtag</see>
    [<CustomOperation "eTag">]
    member _.ETag (state : UpsertOperation<_>, eTag : string) =
        state.RequestOptions.IfMatchEtag <- eTag
        state

type UpsertConcurrentlyBuilder<'a, 'e> (enableContentResponseOnWrite : bool) =
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
        : UpsertConcurrentlyOperation<'a, 'e>

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
    member _.UpdateOrCreate (state : UpsertConcurrentlyOperation<_, _>, update : 'a option -> Async<Result<'a, 't>>) = {
        state with
            UpdateOrCreate = update
    }

let upsert<'a> = UpsertBuilder<'a> (false)
let upsertAndRead<'a> = UpsertBuilder<'a> (true)

let upsertConcurrenly<'a, 'e> = UpsertConcurrentlyBuilder<'a, 'e> (false)
let upsertConcurrenlyAndRead<'a, 'e> = UpsertConcurrentlyBuilder<'a, 'e> (true)

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type UpsertResult<'t> =
    | Ok of 't // 200
    | BadRequest of ResponseBody : string // 400
    | ModifiedBefore of ResponseBody : string // 412 - need re-do
    | EntityTooLarge of ResponseBody : string // 413
    | TooManyRequests of ResponseBody : string * RetryAfter : TimeSpan voption // 429

type UpsertConcurrentResult<'t, 'e> =
    | Ok of 't // 200
    | BadRequest of ResponseBody : string // 400
    | ModifiedBefore of ResponseBody : string //412 - need re-do
    | EntityTooLarge of ResponseBody : string // 413
    | TooManyRequests of ResponseBody : string * RetryAfter : TimeSpan voption // 429
    | CustomError of Error : 'e

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

    member container.PlainExecuteAsync<'a>
        (operation : UpsertOperation<'a>, [<Optional>] cancellationToken : CancellationToken)
        =
        container.UpsertItemAsync<'a> (
            operation.Item,
            operation.PartitionKey |> ValueOption.toNullable,
            operation.RequestOptions,
            cancellationToken = cancellationToken
        )

    member private container.ExecuteCoreAsync<'a>
        (operation : UpsertOperation<'a>, [<Optional>] cancellationToken : CancellationToken)
        =
        task {
            try
                let! response = container.PlainExecuteAsync (operation, cancellationToken)
                return CosmosResponse.fromItemResponse UpsertResult.Ok response
            with HandleException ex ->
                return CosmosResponse.fromException toUpsertResult ex
        }

    member container.ExecuteAsync<'a> (operation : UpsertOperation<'a>, [<Optional>] cancellationToken : CancellationToken) =
        if String.IsNullOrEmpty operation.RequestOptions.IfMatchEtag then
            invalidArg "eTag" "Safe replace requires ETag"

        container.ExecuteCoreAsync (operation, cancellationToken)

    member container.ExecuteOverwriteAsync<'a>
        (operation : UpsertOperation<'a>, [<Optional>] cancellationToken : CancellationToken)
        =
        container.ExecuteCoreAsync (operation, cancellationToken)

    member container.ExecuteConcurrentlyAsync<'a, 'e>
        (
            operation : UpsertConcurrentlyOperation<'a, 'e>,
            [<Optional; DefaultParameterValue(DefaultMaxRetryCount)>] maxRetryCount : int,
            [<Optional>] cancellationToken : CancellationToken
        )
        =
        executeConcurrentlyAsync<'a, 'e> cancellationToken container operation maxRetryCount

    member container.ExecuteConcurrentlyAsync<'a, 'e>
        (operation : UpsertConcurrentlyOperation<'a, 'e>, [<Optional>] cancellationToken : CancellationToken)
        =
        executeConcurrentlyAsync<'a, 'e> cancellationToken container operation DefaultMaxRetryCount
