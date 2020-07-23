[<AutoOpen>]
module FSharp.Azure.Cosmos.Builders

open System
open Microsoft.Azure.Cosmos

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

type ReplaceBuilder<'a>() =
    member __.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            Id = String.Empty
            PartitionKey = ValueNone
            RequestOptions = ValueNone
        } : ReplaceOperation<'a>

    /// Sets the item being to replace existing with
    [<CustomOperation "item">]
    member __.Item (state : ReplaceOperation<_>, item) = { state with Item = item }

    /// Sets the item being to replace existing with
    [<CustomOperation "id">]
    member __.Id (state : ReplaceOperation<_>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member __.PartitionKey (state : ReplaceOperation<_>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member __.PartitionKeyValue (state : ReplaceOperation<_>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member __.RequestOptions (state : ReplaceOperation<_>, options: ItemRequestOptions) = { state with RequestOptions = ValueSome options }

    /// Sets the eTag
    [<CustomOperation "eTagValue">]
    member __.ETagValue (state : ReplaceOperation<_>, eTag: string) =
        match state.RequestOptions with
        | ValueSome options ->
            options.IfMatchEtag <- eTag
            state
        | ValueNone ->
            let options = ItemRequestOptions (IfMatchEtag = eTag)
            { state with RequestOptions = ValueSome options }

type UpsertBuilder<'a>() =
    member __.Yield _ =
        {
            Item = Unchecked.defaultof<_>
            PartitionKey = ValueNone
            RequestOptions = ValueNone
        } : UpsertOperation<'a>

    /// Sets the item being creeated
    [<CustomOperation "item">]
    member __.Item (state : UpsertOperation<_>, item) = { state with Item = item }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member __.PartitionKey (state : UpsertOperation<_>, partitionKey: PartitionKey) = { state with PartitionKey = ValueSome partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member __.PartitionKeyValue (state : UpsertOperation<_>, partitionKey: string) = { state with PartitionKey = ValueSome (PartitionKey partitionKey) }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member __.RequestOptions (state : UpsertOperation<_>, options: ItemRequestOptions) = { state with RequestOptions = ValueSome options }

    /// Sets the eTag
    [<CustomOperation "eTagValue">]
    member __.ETagValue (state : UpsertOperation<_>, eTag: string) =
        match state.RequestOptions with
        | ValueSome options ->
            options.IfMatchEtag <- eTag
            state
        | ValueNone ->
            let options = ItemRequestOptions (IfMatchEtag = eTag)
            { state with RequestOptions = ValueSome options }

type DeleteBuilder() =
    member __.Yield _ =
        {
            Id = String.Empty
            PartitionKey = Unchecked.defaultof<_>
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

let create<'a> = CreateBuilder<'a>()
let replace<'a> = ReplaceBuilder<'a>()
let upsert<'a> = UpsertBuilder<'a>()
let delete = DeleteBuilder()
let read<'a> = ReadBuilder<'a>()
