[<AutoOpen>]
module FSharp.Azure.Cosmos.Create

open Microsoft.Azure.Cosmos

[<Struct>]
type CreateOperation<'a> =
    { Item : 'a
      PartitionKey : PartitionKey voption
      RequestOptions : ItemRequestOptions voption }

type CreateBuilder<'a>() =
    member _.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            PartitionKey = ValueNone
            RequestOptions = ValueNone
        } : CreateOperation<'a>

    /// Sets the item being creeated
    [<CustomOperation "item">]
    member _.Item (state : inref<CreateOperation<_>>, item) = { state with Item = item }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : inref<CreateOperation<_>>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member _.PartitionKeyValue (state : inref<CreateOperation<_>>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : inref<CreateOperation<_>>, options: ItemRequestOptions) = { state with RequestOptions = ValueSome options }

    /// Enable content response on write
    member private _.EnableContentResponseOnWrite (state : inref<CreateOperation<_>>, enable) =
        match state.RequestOptions with
        | ValueSome options ->
            options.EnableContentResponseOnWrite <- enable
            state
        | ValueNone ->
            let options = ItemRequestOptions (EnableContentResponseOnWrite = enable)
            { state with RequestOptions = ValueSome options }

    /// Enables content response on write
    [<CustomOperation "enableContentResponseOnWrite">]
    member this.EnableContentResponseOnWrite (state : inref<CreateOperation<_>>) = this.EnableContentResponseOnWrite (state, true)

    /// Disanables content response on write
    [<CustomOperation "disableContentResponseOnWrite">]
    member this.DisableContentResponseOnWrite (state : inref<CreateOperation<_>>) = this.EnableContentResponseOnWrite (state, false)

let create<'a> = CreateBuilder<'a>()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type CreateResult<'t> =
    | Ok of 't // 201
    | BadRequest of ResponseBody : string // 400
    | Conflict of ResponseBody : string // 409
    | EntityTooLarge of ResponseBody : string // 413

open System.Net

let private toCreateResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.BadRequest -> CreateResult.BadRequest ex.ResponseBody
    | HttpStatusCode.Conflict -> CreateResult.Conflict ex.ResponseBody
    | HttpStatusCode.RequestEntityTooLarge -> CreateResult.EntityTooLarge ex.ResponseBody
    | _ -> raise ex

type Microsoft.Azure.Cosmos.Container with

    member container.AsyncExecute<'a> (operation : CreateOperation<'a>) : Async<CosmosResponse<CreateResult<'a>>> = async {
        if operation.Item = Unchecked.defaultof<'a> then invalidArg "item" "No item to create specified"

        let! ct = Async.CancellationToken
        try
            let! response = container.CreateItemAsync<'a>(operation.Item,
                                                          operation.PartitionKey |> ValueOption.toNullable,
                                                          operation.RequestOptions |> ValueOption.toObj,
                                                          cancellationToken = ct)
            return CosmosResponse.fromItemResponse CreateResult.Ok response
        with
        | HandleException ex -> return CosmosResponse.fromException toCreateResult ex
    }
