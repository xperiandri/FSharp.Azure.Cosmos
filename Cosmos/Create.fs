[<AutoOpen>]
module FSharp.Azure.Cosmos.Create

open System
open Microsoft.Azure.Cosmos

[<Struct>]
type CreateOperation<'a> = {
    Item : 'a
    PartitionKey : PartitionKey voption
    RequestOptions : ItemRequestOptions
}

type CreateBuilder<'a> (enableContentResponseOnWrite : bool) =
    member _.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            PartitionKey = ValueNone
            RequestOptions = ItemRequestOptions (EnableContentResponseOnWrite = enableContentResponseOnWrite)
        }
        : CreateOperation<'a>

    /// Sets the item being created
    [<CustomOperation "item">]
    member _.Item (state : CreateOperation<_>, item) = { state with Item = item }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : CreateOperation<_>, partitionKey : PartitionKey) = {
        state with
            PartitionKey = ValueSome partitionKey
    }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : CreateOperation<_>, partitionKey : string) = {
        state with
            PartitionKey = ValueSome (PartitionKey partitionKey)
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : CreateOperation<_>, options : ItemRequestOptions) =
        options.EnableContentResponseOnWrite <- enableContentResponseOnWrite
        { state with RequestOptions = options }

let create<'a> = CreateBuilder<'a> (false)
let createAndRead<'a> = CreateBuilder<'a> (true)

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type CreateResult<'t> =
    | Ok of 't // 201
    | BadRequest of ResponseBody : string // 400
    | Conflict of ResponseBody : string // 409
    | EntityTooLarge of ResponseBody : string // 413
    | TooManyRequests of ResponseBody : string * RetryAfter : TimeSpan voption // 429

open System.Net
let private toCreateResult (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.BadRequest -> CreateResult.BadRequest ex.ResponseBody
    | HttpStatusCode.Conflict -> CreateResult.Conflict ex.ResponseBody
    | HttpStatusCode.RequestEntityTooLarge -> CreateResult.EntityTooLarge ex.ResponseBody
    | HttpStatusCode.TooManyRequests -> CreateResult.TooManyRequests (ex.ResponseBody, ex.RetryAfter |> ValueOption.ofNullable)
    | _ -> raise ex

open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks

type Microsoft.Azure.Cosmos.Container with

    member container.PlainExecuteAsync<'a> (operation : CreateOperation<'a>, [<Optional>] cancellationToken : CancellationToken) =
        container.CreateItemAsync<'a> (
            operation.Item,
            operation.PartitionKey |> ValueOption.toNullable,
            operation.RequestOptions,
            cancellationToken = cancellationToken
        )

    member container.ExecuteAsync<'a>
        (operation : CreateOperation<'a>, [<Optional>] cancellationToken : CancellationToken)
        : Task<CosmosResponse<CreateResult<'a>>>
        =
        task {
            try
                let! response = container.PlainExecuteAsync (operation, cancellationToken)
                return CosmosResponse.fromItemResponse CreateResult.Ok response
            with HandleException ex ->
                return CosmosResponse.fromException toCreateResult ex
        }
