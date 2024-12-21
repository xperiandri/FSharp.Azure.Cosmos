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
    member _.Id (state : DeleteOperation, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : DeleteOperation, partitionKey : PartitionKey) = {
        state with
            PartitionKey = partitionKey
    }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : DeleteOperation, partitionKey : string) = {
        state with
            PartitionKey = PartitionKey partitionKey
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : DeleteOperation, options : ItemRequestOptions) = {
        state with
            RequestOptions = ValueSome options
    }

    /// <summary>Sets the eTag to <see cref="ItemRequestOptions.IfNotMatchEtag"/></summary>
    [<CustomOperation "eTag">]
    member _.ETag (state : DeleteOperation, eTag : string) =
        match state.RequestOptions with
        | ValueSome requestOptions ->
            requestOptions.IfNoneMatchEtag <- eTag
            state
        | ValueNone ->
            let options = ItemRequestOptions (IfNoneMatchEtag = eTag)
            { state with RequestOptions = ValueSome options }

    // ------------------------------------------- Request options -------------------------------------------
    /// <summary>Sets the operation <see cref="ConsistencyLevel"/></summary>
    [<CustomOperation "consistencyLevel">]
    member _.ConsistencyLevel (state : DeleteOperation, consistencyLevel : ConsistencyLevel Nullable) =
        match state.RequestOptions with
        | ValueSome requestOptions ->
            requestOptions.ConsistencyLevel <- consistencyLevel
            state
        | ValueNone ->
            let options = ItemRequestOptions (ConsistencyLevel = consistencyLevel)
            { state with RequestOptions = ValueSome options }

    /// Sets if the response should include the content of the item after the operation
    [<CustomOperation "enableContentResponseOnWrite">]
    member _.EnableContentResponseOnWrite (state : DeleteOperation, enableContentResponseOnWrite : bool) =
        match state.RequestOptions with
        | ValueSome requestOptions ->
            requestOptions.EnableContentResponseOnWrite <- enableContentResponseOnWrite
            state
        | ValueNone ->
            let options = ItemRequestOptions (EnableContentResponseOnWrite = enableContentResponseOnWrite)
            { state with RequestOptions = ValueSome options }

    /// Sets the indexing directive
    [<CustomOperation "indexingDirective">]
    member _.IndexingDirective (state : DeleteOperation, indexingDirective : IndexingDirective Nullable) =
        match state.RequestOptions with
        | ValueSome requestOptions ->
            requestOptions.IndexingDirective <- indexingDirective
            state
        | ValueNone ->
            let options = ItemRequestOptions (IndexingDirective = indexingDirective)
            { state with RequestOptions = ValueSome options }

    /// Adds a trigger to be invoked before the operation
    [<CustomOperation "preTrigger">]
    member _.PreTrigger (state : DeleteOperation, trigger : string) =
        match state.RequestOptions with
        | ValueSome requestOptions ->
            requestOptions.AddPreTrigger trigger
            state
        | ValueNone ->
            let options = ItemRequestOptions ()
            options.AddPreTrigger trigger
            { state with RequestOptions = ValueSome options }

    /// Adds triggers to be invoked before the operation
    [<CustomOperation "preTriggers">]
    member _.PreTriggers (state : DeleteOperation, triggers : seq<string>) =
        match state.RequestOptions with
        | ValueSome requestOptions ->
            requestOptions.AddPreTriggers triggers
            state
        | ValueNone ->
            let options = ItemRequestOptions ()
            options.AddPreTriggers triggers
            { state with RequestOptions = ValueSome options }

    /// Adds a trigger to be invoked after the operation
    [<CustomOperation "postTrigger">]
    member _.PostTrigger (state : DeleteOperation, trigger : string) =
        match state.RequestOptions with
        | ValueSome requestOptions ->
            requestOptions.AddPostTrigger trigger
            state
        | ValueNone ->
            let options = ItemRequestOptions ()
            options.AddPostTrigger trigger
            { state with RequestOptions = ValueSome options }

    /// Adds triggers to be invoked after the operation
    [<CustomOperation "postTriggers">]
    member _.PostTriggers (state : DeleteOperation, triggers : seq<string>) =
        match state.RequestOptions with
        | ValueSome requestOptions ->
            requestOptions.AddPostTriggers triggers
            state
        | ValueNone ->
            let options = ItemRequestOptions ()
            options.AddPostTriggers triggers
            { state with RequestOptions = ValueSome options }

    /// Sets the session token
    [<CustomOperation "sessionToken">]
    member _.SessionToken (state : DeleteOperation, sessionToken : string) =
        match state.RequestOptions with
        | ValueSome requestOptions ->
            requestOptions.SessionToken <- sessionToken
            state
        | ValueNone ->
            let options = ItemRequestOptions (SessionToken = sessionToken)
            { state with RequestOptions = ValueSome options }

let delete = DeleteBuilder ()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

/// Represents the result of a delete operation.
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

    /// <summary>
    /// Executes a delete operation
    /// </summary>
    /// <param name="operation">Delete operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.PlainExecuteAsync (operation : DeleteOperation, [<Optional>] cancellationToken : CancellationToken) =
        container.DeleteItemAsync (
            operation.Id,
            operation.PartitionKey,
            operation.RequestOptions |> ValueOption.toObj,
            cancellationToken = cancellationToken
        )

    /// <summary>
    /// Executes a delete operation and returns <see cref="CosmosResponse{DeleteResult}"/>.
    /// </summary>
    /// <param name="operation">Delete operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    member container.ExecuteAsync (operation : DeleteOperation, [<Optional>] cancellationToken : CancellationToken) = task {
        try
            let! response = container.PlainExecuteAsync (operation, cancellationToken)
            return CosmosResponse.fromItemResponse DeleteResult.Ok response
        with HandleException ex ->
            return CosmosResponse.fromException toDeleteResult ex
    }
