[<AutoOpen>]
module FSharp.Azure.Cosmos.Upsert

open Microsoft.Azure.Cosmos

[<Struct>]
type UpsertOperation<'a> =
    { Item : 'a
      PartitionKey : PartitionKey voption
      RequestOptions : ItemRequestOptions voption }

[<Struct>]
type UpsertConcurrentlyOperation<'a, 'e> =
    { Id : string
      PartitionKey : PartitionKey voption
      Update : 'a -> Async<Result<'a, 'e>> }

open System

type UpsertBuilder<'a>() =
    member __.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            PartitionKey = ValueNone
            RequestOptions = ValueNone
        } : UpsertOperation<'a>

    /// Sets the item being creeated
    [<CustomOperation "item">]
    member __.Item (state : UpsertOperation<_>, item) = { state with Item = item }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member __.PartitionKey (state : UpsertOperation<_>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member __.PartitionKeyValue (state : UpsertOperation<_>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member __.RequestOptions (state : UpsertOperation<_>, options: ItemRequestOptions) = { state with RequestOptions = ValueSome options }

    /// Sets the eTag
    [<CustomOperation "eTagValue">]
    member __.ETagValue (state : UpsertOperation<_>, eTag: string) =
        match state.RequestOptions with
        | ValueSome options ->
            options.IfMatchEtag <- eTag
            state
        | ValueNone ->
            let options = ItemRequestOptions (IfMatchEtag = eTag)
            { state with RequestOptions = ValueSome options }

type UpsertConcurrentlyBuilder<'a, 'e>() =
    member __.Yield _ =
        {
            Id = String.Empty
            PartitionKey = ValueNone
            Update = fun u -> async { return Result<'a, 'e>.Ok u }
        } : UpsertConcurrentlyOperation<'a, 'e>

    /// Sets the item being to upsert existing with
    [<CustomOperation "id">]
    member __.Id (state : UpsertConcurrentlyOperation<_,_>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member __.PartitionKey (state : UpsertConcurrentlyOperation<_,_>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member __.PartitionKeyValue (state : UpsertConcurrentlyOperation<_,_>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

    /// Sets the partition key
    [<CustomOperation "update">]
    member __.Update (state : UpsertConcurrentlyOperation<_,_>, update: 'a->Async<Result<'a, 't>>) =  state

let upsert<'a> = UpsertBuilder<'a>()
let upsertConcurrenly<'a, 'e> = UpsertConcurrentlyBuilder<'a, 'e>()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type UpsertResult<'t> =
    | Ok of 't
    | BadRequest of ResponseBody : string // 400
    | ModifiedBefore of ResponseBody : string // 412 - need re-do
    | EntityTooLarge of ResponseBody : string // 413

type UpsertConcurrentResult<'t, 'e> =
    | Ok of 't
    | BadRequest of ResponseBody : string // 400
    | EntityTooLarge of ResponseBody : string // 413
    | TooManyAttempts of AttemptsCount : int // 429
    | CustomError of Error : 'e

open System.Net

let private toUpsertResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.BadRequest -> UpsertResult.BadRequest ex.ResponseBody
    | HttpStatusCode.PreconditionFailed  -> UpsertResult.ModifiedBefore ex.ResponseBody
    | HttpStatusCode.RequestEntityTooLarge -> UpsertResult.EntityTooLarge ex.ResponseBody
    | _ -> raise ex

let private toUpsertConcurrentlyErrorResult (ex : CosmosException) =
    match ex.StatusCode  with
    | HttpStatusCode.BadRequest            -> UpsertConcurrentResult.BadRequest     ex.ResponseBody
    | HttpStatusCode.RequestEntityTooLarge -> UpsertConcurrentResult.EntityTooLarge ex.ResponseBody
    | _ -> raise ex

open System.Runtime.InteropServices

type Microsoft.Azure.Cosmos.Container with

    member private container.AsyncExecute<'a> (getOptions, operation : UpsertOperation<'a>) = async {
        let options = getOptions operation.RequestOptions
        let! ct = Async.CancellationToken
        try
            let! response = container.UpsertItemAsync<'a>(operation.Item,
                                                          operation.PartitionKey |> ValueOption.toNullable,
                                                          options,
                                                          cancellationToken = ct)
            return CosmosResponse.fromItemResponse UpsertResult.Ok response
        with
        | HandleException ex -> return CosmosResponse.fromException toUpsertResult ex
    }

    member container.AsyncExecute<'a> (operation : UpsertOperation<'a>) =
        container.AsyncExecute (getOptions, operation)

    member container.AsyncExecuteUnsafe<'a> (operation : UpsertOperation<'a>) =
        container.AsyncExecute (ValueOption.toObj, operation)

    member container.AsyncExecuteConcurrently<'a,'e> (operation : UpsertConcurrentlyOperation<'a,'e>, [<Optional;DefaultParameterValue(10)>] maxRetryCount : int) =
        asyncExecuteConcurrently<UpsertConcurrentlyOperation<'a,'e>, UpsertConcurrentResult<'a, 'e>, 'a, 'e>
            (toUpsertConcurrentlyErrorResult, Ok, CustomError)
            container
            (operation, maxRetryCount, 0)
