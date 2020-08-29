[<AutoOpen>]
module FSharp.Azure.Cosmos.Read

open Microsoft.Azure.Cosmos

[<Struct>]
type ReadOperation<'a> =
    { Id : string
      PartitionKey : PartitionKey
      RequestOptions : ItemRequestOptions voption }

open System

type ReadBuilder<'a>() =
    member __.Yield _ =
        {
            Id = String.Empty
            PartitionKey = Unchecked.defaultof<_>
            RequestOptions = ValueNone
        } : ReadOperation<'a>

    /// Sets the item being creeated
    [<CustomOperation "id">]
    member __.Id (state : ReadOperation<_>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member __.PartitionKey (state : ReadOperation<_>, partitionKey: PartitionKey) = { state with PartitionKey = partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member __.PartitionKeyValue (state : ReadOperation<_>, partitionKey: string) = { state with PartitionKey = PartitionKey partitionKey }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member __.RequestOptions (state : ReadOperation<_>, options: ItemRequestOptions) = { state with RequestOptions = ValueSome options }

let read<'a> = ReadBuilder<'a>()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type ReadResult<'t> =
    | Ok of 't
    | NotFound of ResponseBody : string // 404

open System.Net

let private toReadResult notFoundResultCtor (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.NotFound -> notFoundResultCtor ex.ResponseBody
    | _ -> raise ex

type Microsoft.Azure.Cosmos.Container with

    member container.AsyncExecute(operation : ReadOperation<'a>, success, failure) = async {
        let! ct = Async.CancellationToken
        try
            let! result = container.ReadItemAsync<'a>(operation.Id,
                                                      operation.PartitionKey,
                                                      operation.RequestOptions |> ValueOption.toObj,
                                                      cancellationToken = ct)
            return CosmosResponse.fromItemResponse (success) result
        with
        | HandleException ex -> return CosmosResponse.fromException (failure) ex
    }

    member container.AsyncExecute (operation : ReadOperation<'a>) =
        container.AsyncExecute (operation, ReadResult.Ok, toReadResult ReadResult.NotFound)

    member container.AsyncExecuteOption (operation : ReadOperation<'a>) =
        container.AsyncExecute (operation, Some, toReadResult (fun _ -> None))

    member container.AsyncExecuteValueOption (operation : ReadOperation<'a>) =
        container.AsyncExecute (operation, ValueSome, toReadResult (fun _ -> ValueNone))

