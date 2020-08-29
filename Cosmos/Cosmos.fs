namespace FSharp.Azure.Cosmos

open System
open System.Net
open FSharp.Control
open Microsoft.Azure.Cosmos

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

    let internal getOptions (options : ItemRequestOptions voption) =
        match options with
        | ValueNone ->
            raise <| ArgumentException ("ETag not specified", "eTag")
        | ValueSome options when String.IsNullOrWhiteSpace options.IfMatchEtag ->
            raise <| ArgumentException ("ETag not specified", "eTag")
        | ValueSome options -> options

    let toCosmosException (ex : Exception) =
        match ex with
            | :? CosmosException as ex -> Some ex
            | :? AggregateException as ex ->
                match ex.InnerException with
                | :? CosmosException as cex -> Some cex
                | _ -> None
            | _ -> None

    let handleException (ex : Exception) =
        let cosmosException = toCosmosException ex
        match cosmosException with
            | Some ex when handleStatusCode ex.StatusCode -> Some ex
            | _ -> None

    let (|CosmosException|_|) (ex : Exception) =
        toCosmosException ex

    let (|HandleException|_|) (ex : Exception) =
        handleException ex


    let inline asyncExecuteConcurrently< ^operation, ^result, 'value, 'error
                                    when ^operation : (member Id : string)
                                     and ^operation : (member PartitionKey : PartitionKey voption)
                                     and ^operation : (member Update : FSharpFunc<'value, Async<Result<'value, 'error>>>)>
        (toErrorResult : CosmosException -> ^result, okCtor, customErrorCtor)
        (container : Container)
        (operation : ^operation, maxRetryCount : int, currentAttemptCount : int) : Async<CosmosResponse< ^result>> =

        let rec asyncExecuteConcurrently (toErrorResult, okCtor, customErrorCtor)
                                         (container : Container)
                                         (operation, maxRetryCount, currentAttemptCount) = async {

            let retryUpdate (e : CosmosException) =
                match e.StatusCode with
                | HttpStatusCode.PreconditionFailed when currentAttemptCount >= maxRetryCount ->
                    CosmosResponse.fromException toErrorResult e |> async.Return
                | HttpStatusCode.PreconditionFailed ->
                    asyncExecuteConcurrently (toErrorResult, okCtor, customErrorCtor) container (operation, maxRetryCount, currentAttemptCount + 1)
                | _ ->
                    CosmosResponse.fromException toErrorResult e |> async.Return

            let! ct = Async.CancellationToken

            try
                let! response = container.ReadItemAsync<'value>((^operation : (member Id : string) operation),
                                                            (^operation : (member PartitionKey : PartitionKey voption) operation).Value,
                                                            cancellationToken = ct)
                let eTag = response.ETag
                let update = (^operation : (member Update : FSharpFunc<'value,Async<Result<'value, 'error>>>) operation)
                let! itemUpdateResult = update response.Resource
                match itemUpdateResult with
                | Result.Error e -> return CosmosResponse.fromItemResponse (fun _ -> customErrorCtor e) response
                | Result.Ok item ->
                    let updateOptions = new ItemRequestOptions(IfMatchEtag = eTag)
                    let! response = container.ReplaceItemAsync<'value>(item, (^operation : (member Id : string) operation), requestOptions = updateOptions, cancellationToken = ct)
                    return CosmosResponse.fromItemResponse okCtor response
            with
            | HandleException ex -> return! retryUpdate ex
        }
        asyncExecuteConcurrently (toErrorResult, okCtor, customErrorCtor) (container) (operation, maxRetryCount, currentAttemptCount)

    type Microsoft.Azure.Cosmos.Container with

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
