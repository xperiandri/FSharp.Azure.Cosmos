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
    member _.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            Id = String.Empty
            PartitionKey = ValueNone
            RequestOptions = ValueNone
        } : ReplaceOperation<'a>

    /// Sets the item being to replace existing with
    [<CustomOperation "item">]
    member _.Item (state : inref<ReplaceOperation<_>>, item) = { state with Item = item }

    /// Sets the item being to replace existing with
    [<CustomOperation "id">]
    member _.Id (state : inref<ReplaceOperation<_>>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : inref<ReplaceOperation<_>>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member _.PartitionKeyValue (state : inref<ReplaceOperation<_>>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : inref<ReplaceOperation<_>>, options: ItemRequestOptions) = { state with RequestOptions = ValueSome options }

    /// Sets the eTag to <see href="IfMatchEtag">IfMatchEtag</see>
    [<CustomOperation "eTagValue">]
    member _.ETagValue (state : inref<ReplaceOperation<_>>, eTag: string) =
        match state.RequestOptions with
        | ValueSome options ->
            options.IfMatchEtag <- eTag
            state
        | ValueNone ->
            let options = ItemRequestOptions (IfMatchEtag = eTag)
            { state with RequestOptions = ValueSome options }

    /// Enable content response on write
    member private _.EnableContentResponseOnWrite (state : inref<ReplaceOperation<_>>, enable) =
        match state.RequestOptions with
        | ValueSome options ->
            options.EnableContentResponseOnWrite <- enable
            state
        | ValueNone ->
            let options = ItemRequestOptions (EnableContentResponseOnWrite = enable)
            { state with RequestOptions = ValueSome options }

    /// Enables content response on write
    [<CustomOperation "enableContentResponseOnWrite">]
    member this.EnableContentResponseOnWrite (state : inref<ReplaceOperation<_>>) = this.EnableContentResponseOnWrite (state, true)

    /// Disanables content response on write
    [<CustomOperation "disableContentResponseOnWrite">]
    member this.DisableContentResponseOnWrite (state : inref<ReplaceOperation<_>>) = this.EnableContentResponseOnWrite (state, false)

type ReplaceConcurrentlyBuilder<'a, 'e>() =
    member _.Yield _ =
        {
            Id = String.Empty
            PartitionKey = ValueNone
            Update = fun _ -> raise <| MissingMethodException ("Update function is not set for concurrent replace operation")
        } : ReplaceConcurrentlyOperation<'a, 'e>

    /// Sets the item being to replace existing with
    [<CustomOperation "id">]
    member _.Id (state : inref<ReplaceConcurrentlyOperation<_,_>>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : inref<ReplaceConcurrentlyOperation<_,_>>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member _.PartitionKeyValue (state : inref<ReplaceConcurrentlyOperation<_,_>>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

    /// Sets the partition key
    [<CustomOperation "update">]
    member _.Update (state : inref<ReplaceConcurrentlyOperation<_,_>>, update : 'a->Async<Result<'a, 't>>) = { state with Update = update }

let replace<'a> = ReplaceBuilder<'a>()
let replaceConcurrenly<'a, 'e> = ReplaceConcurrentlyBuilder<'a, 'e>()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type ReplaceResult<'t> =
    | Ok of 't // 200
    | BadRequest of ResponseBody : string // 400
    | NotFound of ResponseBody : string // 404
    | ModifiedBefore of ResponseBody : string //412 - need re-do
    | EntityTooLarge of ResponseBody : string // 413

type ReplaceConcurrentResult<'t, 'e> =
    | Ok of 't // 200
    | BadRequest of ResponseBody : string // 400
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
        if String.IsNullOrEmpty operation.Id then invalidArg "id" "Replace operation requires Id"
        if operation.Item = Unchecked.defaultof<'a> then invalidArg "item" "No item to replace specified"

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
        match operation.RequestOptions with
        | ValueNone -> invalidArg "eTag" "Safe replace requires ETag"
        | ValueSome options when String.IsNullOrEmpty options.IfMatchEtag -> invalidArg "eTag" "Safe replace requires ETag"
        | _ -> ()
        container.AsyncExecute (getOptions, operation)

    member container.AsyncExecuteOverwrite<'a> (operation : ReplaceOperation<'a>) =
        container.AsyncExecute (ValueOption.toObj, operation)

    member container.AsyncExecuteConcurrently<'a,'e> (operation : ReplaceConcurrentlyOperation<'a,'e>, [<Optional;DefaultParameterValue(10)>] maxRetryCount : int) =
        asyncExecuteConcurrently<'a, 'e> container operation maxRetryCount 0
