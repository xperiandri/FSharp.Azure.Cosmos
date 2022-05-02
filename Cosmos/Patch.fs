[<AutoOpen>]
module FSharp.Azure.Cosmos.Patch

open Microsoft.Azure.Cosmos

[<Struct>]
type PatchOperation =
    { Operations : PatchOp list
      Id : string
      PartitionKey : PartitionKey voption
      RequestOptions : ItemRequestOptions voption }

[<Struct>]
type PatchConcurrentlyOperation<'e> =
    { Operations : PatchOp list
      Id : string
      PartitionKey : PartitionKey voption }

open System

type PatchBuilder() =
    member _.Yield _ =
        {
            Operations = []
            Id = String.Empty
            PartitionKey = ValueNone
            RequestOptions = ValueNone
        } : PatchOperation

    /// Adds the <see href"PatchOp">Patch operation</see>
    [<CustomOperation "operation">]
    member _.Operation (state : inref<PatchOperation>, operation) = { state with Operations = operation :: state.Operations }

    /// Sets the item being to Patch existing with
    [<CustomOperation "id">]
    member _.Id (state : inref<PatchOperation>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : inref<PatchOperation>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member _.PartitionKeyValue (state : inref<PatchOperation>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : inref<PatchOperation>, options: ItemRequestOptions) = { state with RequestOptions = ValueSome options }

    /// Sets the eTag to <see href="IfMatchEtag">IfMatchEtag</see>
    [<CustomOperation "eTagValue">]
    member _.ETagValue (state : inref<PatchOperation>, eTag: string) =
        match state.RequestOptions with
        | ValueSome options ->
            options.IfMatchEtag <- eTag
            state
        | ValueNone ->
            let options = ItemRequestOptions (IfMatchEtag = eTag)
            { state with RequestOptions = ValueSome options }

    /// Enable content response on write
    member private _.EnableContentResponseOnWrite (state : inref<PatchOperation>, enable) =
        match state.RequestOptions with
        | ValueSome options ->
            options.EnableContentResponseOnWrite <- enable
            state
        | ValueNone ->
            let options = ItemRequestOptions (EnableContentResponseOnWrite = enable)
            { state with RequestOptions = ValueSome options }

    /// Enables content response on write
    [<CustomOperation "enableContentResponseOnWrite">]
    member this.EnableContentResponseOnWrite (state : inref<PatchOperation>) = this.EnableContentResponseOnWrite (state, true)

    /// Disanables content response on write
    [<CustomOperation "disableContentResponseOnWrite">]
    member this.DisableContentResponseOnWrite (state : inref<PatchOperation>) = this.EnableContentResponseOnWrite (state, false)

type PatchConcurrentlyBuilder<'e>() =
    member _.Yield _ =
        {
            Operations = []
            Id = String.Empty
            PartitionKey = ValueNone
        } : PatchConcurrentlyOperation<'e>

    /// Adds the <see href"PatchOp">Patch operation</see>
    [<CustomOperation "operation">]
    member _.Operation (state : inref<PatchConcurrentlyOperation<_>>, operation) = { state with Operations = operation :: state.Operations }

    /// Sets the item being to Patch existing with
    [<CustomOperation "id">]
    member _.Id (state : inref<PatchConcurrentlyOperation<_>>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : inref<PatchConcurrentlyOperation<_>>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member _.PartitionKeyValue (state : inref<PatchConcurrentlyOperation<_>>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

let Patch = PatchBuilder()
let PatchConcurrenly<'e> = PatchConcurrentlyBuilder<'e>()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type PatchResult<'t> =
    | Ok of 't // 200
    | NotExecuted
    | BadRequest of ResponseBody : string // 400
    | NotFound of ResponseBody : string // 404
    | ModifiedBefore of ResponseBody : string //412 - need re-do

type PatchConcurrentResult<'t, 'e> =
    | Ok of 't // 200
    | NotExecuted
    | BadRequest of ResponseBody : string // 400
    | NotFound of ResponseBody : string // 404
    | TooManyAttempts of AttemptsCount : int // 429
    | CustomError of Error : 'e

open System.Net

let private toPatchResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.BadRequest -> PatchResult.BadRequest ex.ResponseBody
    | HttpStatusCode.NotFound -> PatchResult.NotFound ex.ResponseBody
    | HttpStatusCode.PreconditionFailed  -> PatchResult.ModifiedBefore ex.ResponseBody
    | _ -> raise ex

let private toPatchConcurrentlyErrorResult (ex : CosmosException) =
    match ex.StatusCode  with
    | HttpStatusCode.NotFound              -> PatchConcurrentResult.NotFound       ex.ResponseBody
    | HttpStatusCode.BadRequest            -> PatchConcurrentResult.BadRequest     ex.ResponseBody
    | _ -> raise ex


let rec asyncExecuteConcurrently<'error>
        (container : Container)
        (operation : PatchConcurrentlyOperation<'error>)
        (maxRetryCount : int)
        (currentAttemptCount : int) : Async<CosmosResponse<PatchConcurrentResult<'error>>> = async {

    let retryUpdate =
        retryUpdate toPatchConcurrentlyErrorResult
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
            let! response = container.PatchItemAsync<'value>(item, operation.Id, requestOptions = updateOptions, cancellationToken = ct)
            return CosmosResponse.fromItemResponse Ok response
    with
    | HandleException ex -> return! retryUpdate ex
}

open System.Runtime.InteropServices

type Microsoft.Azure.Cosmos.Container with

    member private container.AsyncExecute (operation : inref<PatchOperation>) = async {
        if String.IsNullOrEmpty operation.Id then invalidArg "id" "Patch operation requires Id"
        if operation.Operations = Unchecked.defaultof<'a> then invalidArg "item" "No item to Patch specified"

        let! ct = Async.CancellationToken
        try
            let! response = container.PatchItemAsync<'a>(operation.Item,
                                                           operation.Id,
                                                           operation.PartitionKey |> ValueOption.toNullable,
                                                           operation.RequestOptions |> ValueOption.toObj,
                                                           cancellationToken = ct)
            return CosmosResponse.fromItemResponse PatchResult.Ok response
        with
        | HandleException ex -> return CosmosResponse.fromException toPatchResult ex
    }

    member container.AsyncExecute =
        match operation.RequestOptions with
        | ValueNone -> invalidArg "eTag" "Safe Patch requires ETag"
        | ValueSome options when String.IsNullOrEmpty options.IfMatchEtag -> invalidArg "eTag" "Safe Patch requires ETag"
        | _ -> ()
        container.AsyncExecute operation

    member container.AsyncExecuteOverwrite<'a> (operation : inref<PatchOperation><'a>) =
        container.AsyncExecute (ValueOption.toObj, operation)

    member container.AsyncExecuteConcurrently<'e> (operation : PatchConcurrentlyOperation<'e>, [<Optional;DefaultParameterValue(10)>] maxRetryCount : int) =
        asyncExecuteConcurrently<'e> container operation maxRetryCount 0
