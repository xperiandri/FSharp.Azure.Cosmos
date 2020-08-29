[<AutoOpen>]
module FSharp.Azure.Cosmos.Create

open Microsoft.Azure.Cosmos

[<Struct>]
type CreateOperation<'a> =
    { Item : 'a
      PartitionKey : PartitionKey voption
      RequestOptions : ItemRequestOptions voption }

type CreateBuilder<'a>() =
    member __.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            PartitionKey = ValueNone
            RequestOptions = ValueNone
        } : CreateOperation<'a>

    /// Sets the item being creeated
    [<CustomOperation "item">]
    member __.Item (state : CreateOperation<_>, item) = { state with Item = item }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member __.PartitionKey (state : CreateOperation<_>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member __.PartitionKeyValue (state : CreateOperation<_>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member __.RequestOptions (state : CreateOperation<_>, options: ItemRequestOptions) = { state with RequestOptions = ValueSome options }

let create<'a> = CreateBuilder<'a>()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type CreateResult<'t> =
    | Ok of 't
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
