namespace FSharp.Azure.Cosmos

open Microsoft.Azure.Cosmos

[<Struct>]
type CreateOperation<'a> =
    { Item : 'a
      PartitionKey : PartitionKey voption
      RequestOptions : ItemRequestOptions voption }

[<Struct>]
type ReplaceOperation<'a> =
    { Item : 'a
      Id : string
      PartitionKey : PartitionKey voption
      RequestOptions : ItemRequestOptions voption }

[<Struct>]
type UpsertOperation<'a> =
    { Item : 'a
      PartitionKey : PartitionKey voption
      RequestOptions : ItemRequestOptions voption }

[<Struct>]
type DeleteOperation =
    { Id : string
      PartitionKey : PartitionKey
      RequestOptions : ItemRequestOptions voption }

[<Struct>]
type ReadOperation<'a> =
    { Id : string
      PartitionKey : PartitionKey
      RequestOptions : ItemRequestOptions voption }


open System
open FSharp.Control

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

type CreateResult<'t> =
    | Ok of 't
    | BadRequest of ResponseBody : string // 400
    | Conflict of ResponseBody : string // 409
    | EntityTooLarge of ResponseBody : string // 413

type ReplaceResult<'t> =
    | Ok of 't
    | BadRequest of ResponseBody : string // 400
    | NotFound of ResponseBody : string // 404
    | Conflict of ResponseBody : string // 409
    | ModifiedBefore of ResponseBody : string //412 - need re-do
    | EntityTooLarge of ResponseBody : string // 413

type UpsertResult<'t> =
    | Ok of 't
    | BadRequest of ResponseBody : string // 400
    | Conflict of ResponseBody : string // 409
    | ModifiedBefore of ResponseBody : string // 412 - need re-do
    | EntityTooLarge of ResponseBody : string // 413

type DeleteResult<'t> =
    | Ok of 't
    | NotFound of ResponseBody : string // 404

type ReadResult<'t> =
    | Ok of 't
    | NotFound of ResponseBody : string // 404

open System.Net
open System.Runtime.InteropServices

[<AutoOpen>]
module Operations =

    let handleStatusCode statusCode =
        match statusCode with
        | HttpStatusCode.BadRequest
        | HttpStatusCode.NotFound
        | HttpStatusCode.Conflict
        | HttpStatusCode.PreconditionFailed
        | HttpStatusCode.RequestEntityTooLarge -> true
        | _ -> false

    let private toCreateResult (ex : CosmosException) =
        match ex.StatusCode with
        | HttpStatusCode.BadRequest -> CreateResult.BadRequest ex.ResponseBody
        | HttpStatusCode.Conflict -> CreateResult.Conflict ex.ResponseBody
        | HttpStatusCode.RequestEntityTooLarge -> CreateResult.EntityTooLarge ex.ResponseBody
        | _ -> raise ex

    let private toReplaceResult (ex : CosmosException) =
        match ex.StatusCode with
        | HttpStatusCode.BadRequest -> ReplaceResult.BadRequest ex.ResponseBody
        | HttpStatusCode.NotFound -> ReplaceResult.NotFound ex.ResponseBody
        | HttpStatusCode.Conflict -> ReplaceResult.Conflict ex.ResponseBody
        | HttpStatusCode.PreconditionFailed  -> ReplaceResult.ModifiedBefore ex.ResponseBody
        | HttpStatusCode.RequestEntityTooLarge -> ReplaceResult.EntityTooLarge ex.ResponseBody
        | _ -> raise ex

    let private toUpsertResult (ex : CosmosException) =
        match ex.StatusCode with
        | HttpStatusCode.BadRequest -> UpsertResult.BadRequest ex.ResponseBody
        | HttpStatusCode.Conflict -> UpsertResult.Conflict ex.ResponseBody
        | HttpStatusCode.PreconditionFailed  -> UpsertResult.ModifiedBefore ex.ResponseBody
        | HttpStatusCode.RequestEntityTooLarge -> UpsertResult.EntityTooLarge ex.ResponseBody
        | _ -> raise ex

    let private toDeleteResult (ex : CosmosException) =
        match ex.StatusCode with
        | HttpStatusCode.NotFound -> DeleteResult.NotFound ex.ResponseBody
        | _ -> raise ex

    let private toReadResult notFoundResultCtor (ex : CosmosException) =
        match ex.StatusCode with
        | HttpStatusCode.NotFound -> notFoundResultCtor ex.ResponseBody
        | _ -> raise ex

    let private getOptions (options : ItemRequestOptions voption) =
        match options with
        | ValueNone->
            raise <| ArgumentException ("ETag not specified", "eTag")
        | ValueSome options when String.IsNullOrWhiteSpace options.IfMatchEtag ->
            raise <| ArgumentException ("ETag not specified", "eTag")
        | ValueSome options -> options

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
            | :? AggregateException as ex
                when (match ex.InnerException with
                      | :? CosmosException as cex -> handleStatusCode cex.StatusCode
                      | _ -> false) ->
                    return CosmosResponse.fromException toCreateResult (ex.InnerException :?> CosmosException)
            | :? CosmosException as ex
                when handleStatusCode ex.StatusCode ->
                    return CosmosResponse.fromException toCreateResult ex
        }

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
            | :? AggregateException as ex
                when (match ex.InnerException with
                      | :? CosmosException as cex -> handleStatusCode cex.StatusCode
                      | _ -> false) ->
                    return CosmosResponse.fromException toReplaceResult (ex.InnerException :?> CosmosException)
            | :? CosmosException as ex
                when handleStatusCode ex.StatusCode ->
                    return CosmosResponse.fromException toReplaceResult ex
        }
        member container.AsyncExecute<'a> (operation : ReplaceOperation<'a>) =
            container.AsyncExecute (getOptions, operation)
        member container.AsyncExecuteUnsafe<'a> (operation : ReplaceOperation<'a>) =
            container.AsyncExecute (ValueOption.toObj, operation)

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
            | :? AggregateException as ex
                when (match ex.InnerException with
                      | :? CosmosException as cex -> handleStatusCode cex.StatusCode
                      | _ -> false) ->
                    return CosmosResponse.fromException toUpsertResult (ex.InnerException :?> CosmosException)
            | :? CosmosException as ex
                when handleStatusCode ex.StatusCode ->
                    return CosmosResponse.fromException toUpsertResult ex

        }
        member container.AsyncExecute<'a> (operation : UpsertOperation<'a>) =
            container.AsyncExecute (getOptions, operation)
        member container.AsyncExecuteUnsafe<'a> (operation : UpsertOperation<'a>) =
            container.AsyncExecute (ValueOption.toObj, operation)

        member container.AsyncExecute (operation : DeleteOperation) = async {
            let! ct = Async.CancellationToken
            try
                let! response = container.DeleteItemAsync(operation.Id,
                                                          operation.PartitionKey,
                                                          operation.RequestOptions |> ValueOption.toObj,
                                                          cancellationToken = ct)
                return CosmosResponse.fromItemResponse DeleteResult.Ok response
            with
            | :? AggregateException as ex
                when (match ex.InnerException with
                      | :? CosmosException as cex -> handleStatusCode cex.StatusCode
                      | _ -> false) ->
                    return CosmosResponse.fromException toDeleteResult (ex.InnerException :?> CosmosException)
            | :? CosmosException as ex
                when handleStatusCode ex.StatusCode ->
                    return CosmosResponse.fromException toDeleteResult ex
        }

        member container.AsyncExecute(operation : ReadOperation<'a>, success, failure) = async {
            let! ct = Async.CancellationToken
            try
                let! result = container.ReadItemAsync<'a>(operation.Id,
                                                          operation.PartitionKey,
                                                          operation.RequestOptions |> ValueOption.toObj,
                                                          cancellationToken = ct)
                return CosmosResponse.fromItemResponse (success) result
            with
            | :? AggregateException as ex
                when (match ex.InnerException with
                      | :? CosmosException as cex -> handleStatusCode cex.StatusCode
                      | _ -> false) ->
                    return CosmosResponse.fromException (failure) (ex.InnerException :?> CosmosException)
            | :? CosmosException as ex
                when handleStatusCode ex.StatusCode ->
                    return CosmosResponse.fromException (failure) ex
        }

        member container.AsyncExecute (operation : ReadOperation<'a>) =
            container.AsyncExecute (operation, ReadResult.Ok, toReadResult ReadResult.NotFound)

        member container.AsyncExecuteOption (operation : ReadOperation<'a>) =
            container.AsyncExecute (operation, Some, toReadResult (fun _ -> None))

        member container.AsyncExecuteValueOption (operation : ReadOperation<'a>) =
            container.AsyncExecute (operation, ValueSome, toReadResult (fun _ -> ValueNone))

        member container.AsyncExists (id : string) = async {
            let query =
                QueryDefinition(
                   "SELECT VALUE COUNT(1) \
                    FROM item \
                    WHERE item.id = @ID")
                    .WithParameter("@ID", id)
            let! count =
                container.GetItemQueryIterator<int>(query)
                |> AsyncSeq.ofFeedIterator
                |> AsyncSeq.firstOrDefault 0
            return count = 1
        }

        member container.AsyncIsNotDeleted deletedFieldName (id : string) = async {
            let query =
                QueryDefinition(
                    "SELECT VALUE COUNT(1) \
                     FROM item \
                     WHERE item.id = @ID AND IS_NULL(item."+deletedFieldName+")")
                    .WithParameter("@ID", id)
            let! count =
                container.GetItemQueryIterator<int>(query)
                |> AsyncSeq.ofFeedIterator
                |> AsyncSeq.firstOrDefault 0
            return count = 1
        }
