﻿namespace FSharp.Azure.Cosmos

open System
open System.Net
open FSharp.Control
open Microsoft.Azure.Cosmos

module RequestOptions =

    let internal createOrUpdate setter requestOptions =
        let options =
            match requestOptions with
            | ValueSome options -> options
            | ValueNone -> ItemRequestOptions()
        setter options
        options


[<AutoOpen>]
module Operations =

    let internal canHandleStatusCode statusCode =
        match statusCode with
        | HttpStatusCode.BadRequest
        | HttpStatusCode.NotFound
        | HttpStatusCode.Conflict
        | HttpStatusCode.PreconditionFailed
        | HttpStatusCode.RequestEntityTooLarge -> true
        | _ -> false

    let internal toCosmosException (ex : Exception) =
        match ex with
        | :? CosmosException as ex -> Some ex
        | :? AggregateException as ex ->
            match ex.InnerException with
            | :? CosmosException as cex -> Some cex
            | _ -> None
        | _ -> None

    let internal handleException (ex : Exception) =
        let cosmosException = toCosmosException ex
        match cosmosException with
        | Some ex when canHandleStatusCode ex.StatusCode -> Some ex
        | _ -> None

    let (|CosmosException|_|) (ex : Exception) =
        toCosmosException ex

    let (|HandleException|_|) (ex : Exception) =
        handleException ex

    let internal retryUpdate toErrorResult asyncExecuteConcurrently maxRetryCount currentAttemptCount (e : CosmosException) =
        match e.StatusCode with
        | HttpStatusCode.PreconditionFailed when currentAttemptCount >= maxRetryCount ->
            CosmosResponse.fromException toErrorResult e |> async.Return
        | HttpStatusCode.PreconditionFailed ->
            asyncExecuteConcurrently maxRetryCount (currentAttemptCount + 1)
        | _ ->
            CosmosResponse.fromException toErrorResult e |> async.Return

    type Microsoft.Azure.Cosmos.Container with

        member container.AsyncExists (id : string) = async {
            let query =
                QueryDefinition(
                   "SELECT VALUE COUNT(1) \
                    FROM item \
                    WHERE item.id = @Id")
                    .WithParameter("@Id", id)
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
                     WHERE item.id = @Id AND IS_NULL(item."+deletedFieldName+")")
                    .WithParameter("@Id", id)
            let! count =
                container.GetItemQueryIterator<int>(query)
                |> AsyncSeq.ofFeedIterator
                |> AsyncSeq.firstOrDefault 0
            return count = 1
        }
