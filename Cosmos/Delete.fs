[<AutoOpen>]
module FSharp.Azure.Cosmos.Delete

open Microsoft.Azure.Cosmos

[<Struct>]
type DeleteOperation =
    { Id : string
      PartitionKey : PartitionKey
      RequestOptions : ItemRequestOptions voption }

open System

type DeleteBuilder() =
    member __.Yield _ =
        {
            Id = String.Empty
            PartitionKey = PartitionKey.None
            RequestOptions = ValueNone
        } : DeleteOperation

    /// Sets the item being creeated
    [<CustomOperation "id">]
    member __.Id (state : DeleteOperation, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member __.PartitionKey (state : DeleteOperation, partitionKey: PartitionKey) = { state with PartitionKey = partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member __.PartitionKeyValue (state : DeleteOperation, partitionKey: string) = { state with PartitionKey = PartitionKey partitionKey }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member __.RequestOptions (state : DeleteOperation, options: ItemRequestOptions) = { state with RequestOptions = ValueSome options }

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type DeleteResult<'t> =
    | Ok of 't
    | NotFound of ResponseBody : string // 404

open System.Net

let private toDeleteResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.NotFound -> DeleteResult.NotFound ex.ResponseBody
    | _ -> raise ex

type Microsoft.Azure.Cosmos.Container with

    member container.AsyncExecute (operation : DeleteOperation) = async {
        let! ct = Async.CancellationToken
        try
            let! response = container.DeleteItemAsync(operation.Id,
                                                      operation.PartitionKey,
                                                      operation.RequestOptions |> ValueOption.toObj,
                                                      cancellationToken = ct)
            return CosmosResponse.fromItemResponse DeleteResult.Ok response
        with
        | HandleException ex -> return CosmosResponse.fromException toDeleteResult ex
    }

