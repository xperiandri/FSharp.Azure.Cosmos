[<AutoOpen>]
module FSharp.Azure.Cosmos.Delete

open Microsoft.Azure.Cosmos

[<Struct>]
type DeleteOperation = {
    Id : string
    PartitionKey : PartitionKey
    RequestOptions : ItemRequestOptions voption
}

open System

type DeleteBuilder () =
    member _.Yield _ =
        {
            Id = String.Empty
            PartitionKey = PartitionKey.None
            RequestOptions = ValueNone
        }
        : DeleteOperation

    /// Sets the item being creeated
    [<CustomOperation "id">]
    member _.Id (state : inref<DeleteOperation>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : inref<DeleteOperation>, partitionKey : PartitionKey) = {
        state with
            PartitionKey = partitionKey
    }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : inref<DeleteOperation>, partitionKey : string) = {
        state with
            PartitionKey = PartitionKey partitionKey
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : inref<DeleteOperation>, options : ItemRequestOptions) = {
        state with
            RequestOptions = ValueSome options
    }

let delete = DeleteBuilder ()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type DeleteResult<'t> =
    | Ok of 't // 200
    | NotFound of ResponseBody : string // 404

open System.Net

let private toDeleteResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.NotFound -> DeleteResult.NotFound ex.ResponseBody
    | _ -> raise ex

open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks

type Microsoft.Azure.Cosmos.Container with

    member container.ExecuteAsync (operation : DeleteOperation, [<Optional>] cancellationToken : CancellationToken) = task {
        try
            let! response =
                container.DeleteItemAsync (
                    operation.Id,
                    operation.PartitionKey,
                    operation.RequestOptions |> ValueOption.toObj,
                    cancellationToken = cancellationToken
                )
            return CosmosResponse.fromItemResponse DeleteResult.Ok response
        with HandleException ex ->
            return CosmosResponse.fromException toDeleteResult ex
    }
