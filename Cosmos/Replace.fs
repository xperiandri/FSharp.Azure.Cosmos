[<AutoOpen>]
module FSharp.Azure.Cosmos.Replace

open Microsoft.Azure.Cosmos

[<Struct>]
type ReplaceOperation<'a> =
    { Item : 'a
      Id : string
      PartitionKey : PartitionKey voption
      RequestOptions : ItemRequestOptions voption }

[<Struct>]
type ReplaceConcurrentlyOperation<'a,'e> =
    { Id : string
      PartitionKey : PartitionKey voption
      Update : 'a -> Async<Result<'a,'e>> }

open System

type ReplaceBuilder<'a>() =
    member __.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            Id = String.Empty
            PartitionKey = ValueNone
            RequestOptions = ValueNone
        } : ReplaceOperation<'a>

    /// Sets the item being to replace existing with
    [<CustomOperation "item">]
    member __.Item (state : ReplaceOperation<_>, item) = { state with Item = item }

    /// Sets the item being to replace existing with
    [<CustomOperation "id">]
    member __.Id (state : ReplaceOperation<_>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member __.PartitionKey (state : ReplaceOperation<_>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member __.PartitionKeyValue (state : ReplaceOperation<_>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member __.RequestOptions (state : ReplaceOperation<_>, options: ItemRequestOptions) = { state with RequestOptions = ValueSome options }

    /// Sets the eTag
    [<CustomOperation "eTagValue">]
    member __.ETagValue (state : ReplaceOperation<_>, eTag: string) =
        match state.RequestOptions with
        | ValueSome options ->
            options.IfMatchEtag <- eTag
            state
        | ValueNone ->
            let options = ItemRequestOptions (IfMatchEtag = eTag)
            { state with RequestOptions = ValueSome options }

type ReplaceConcurrentlyBuilder<'a, 'e>() =
    member __.Yield _ =
        {
            Id = String.Empty
            PartitionKey = ValueNone
            Update = fun _ -> raise <| MissingMethodException ("Update function is not set for concurrent upsert operation")
        } : ReplaceConcurrentlyOperation<'a, 'e>

    /// Sets the item being to replace existing with
    [<CustomOperation "id">]
    member __.Id (state : ReplaceConcurrentlyOperation<_,_>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member __.PartitionKey (state : ReplaceConcurrentlyOperation<_,_>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member __.PartitionKeyValue (state : ReplaceConcurrentlyOperation<_,_>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

    /// Sets the partition key
    [<CustomOperation "update">]
    member __.Update (state : ReplaceConcurrentlyOperation<_,_>, update : 'a->Async<Result<'a, 't>>) = { state with Update = update }

let replace<'a> = ReplaceBuilder<'a>()
let replaceConcurrenly<'a, 'e> = ReplaceConcurrentlyBuilder<'a, 'e>()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type ReplaceResult<'t> =
    | Ok of 't
    | BadRequest of ResponseBody : string // 400
    | NotFound of ResponseBody : string // 404
    | ModifiedBefore of ResponseBody : string //412 - need re-do
    | EntityTooLarge of ResponseBody : string // 413

type ReplaceConcurrentResult<'t, 'e> =
    | Ok of 't
    | BadRequest of ResponseBody : string
    | NotFound of ResponseBody : string // 404
    | EntityTooLarge of ResponseBody : string // 413
    | TooManyAttempts of AttemptsCount : int // 429
    | CustomError of Error : 'e

open System.Net

let private toReplaceResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.BadRequest -> ReplaceResult.BadRequest ex.ResponseBody
    | HttpStatusCode.NotFound -> ReplaceResult.NotFound ex.ResponseBody
    | HttpStatusCode.PreconditionFailed  -> ReplaceResult.ModifiedBefore ex.ResponseBody
    | HttpStatusCode.RequestEntityTooLarge -> ReplaceResult.EntityTooLarge ex.ResponseBody
    | _ -> raise ex

let private toReplaceConcurrentlyErrorResult (ex : CosmosException) =
    match ex.StatusCode  with
    | HttpStatusCode.NotFound              -> ReplaceConcurrentResult.NotFound       ex.ResponseBody
    | HttpStatusCode.BadRequest            -> ReplaceConcurrentResult.BadRequest     ex.ResponseBody
    | HttpStatusCode.RequestEntityTooLarge -> ReplaceConcurrentResult.EntityTooLarge ex.ResponseBody
    | _ -> raise ex


let rec asyncExecuteConcurrently<'value, 'error>
        (container : Container)
        (operation : ReplaceConcurrentlyOperation<'value, 'error>)
        (maxRetryCount : int)
        (currentAttemptCount : int) : Async<CosmosResponse<ReplaceConcurrentResult<'value, 'error>>> = async {

    let retryUpdate =
        retryUpdate toReplaceConcurrentlyErrorResult
                    (asyncExecuteConcurrently container operation)
                    maxRetryCount currentAttemptCount

    let! ct = Async.CancellationToken

    try
        let partitionKey =
            match operation.PartitionKey with
            | ValueSome partitionKey -> partitionKey
            | ValueNone -> PartitionKey.None
        let! response = container.ReadItemAsync<'value>(operation.Id, partitionKey, cancellationToken = ct)
        let eTag = response.ETag
        let! itemUpdateResult = operation.Update response.Resource
        match itemUpdateResult with
        | Result.Error e -> return CosmosResponse.fromItemResponse (fun _ -> CustomError e) response
        | Result.Ok item ->
            let updateOptions = new ItemRequestOptions (IfMatchEtag = eTag)
            let! response = container.ReplaceItemAsync<'value>(item, operation.Id, requestOptions = updateOptions, cancellationToken = ct)
            return CosmosResponse.fromItemResponse Ok response
    with
    | HandleException ex -> return! retryUpdate ex
}

open System.Runtime.InteropServices

type Microsoft.Azure.Cosmos.Container with

    member private container.AsyncExecute<'a> (getOptions, operation : ReplaceOperation<'a>) = async {
        let options = getOptions operation.RequestOptions
        let! ct = Async.CancellationToken
        try
            let! response = container.ReplaceItemAsync<'a>(operation.Item,
                                                           operation.Id,
                                                           operation.PartitionKey |> ValueOption.toNullable,
                                                           options,
                                                           cancellationToken = ct)
            return CosmosResponse.fromItemResponse ReplaceResult.Ok response
        with
        | HandleException ex -> return CosmosResponse.fromException toReplaceResult ex
    }

    member container.AsyncExecute<'a> (operation : ReplaceOperation<'a>) =
        container.AsyncExecute (getOptions, operation)

    member container.AsyncExecuteOverwrite<'a> (operation : ReplaceOperation<'a>) =
        container.AsyncExecute (ValueOption.toObj, operation)

    member container.AsyncExecuteConcurrently<'a,'e> (operation : ReplaceConcurrentlyOperation<'a,'e>, [<Optional;DefaultParameterValue(10)>] maxRetryCount : int) =
        asyncExecuteConcurrently<'a, 'e> container operation maxRetryCount 0
