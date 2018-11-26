#####
##### Aggregation
#####

export aggregate

"""
$(SIGNATURES)

Arrange `aggregators` in a named tuple compatible with keys `K`.

*Internal.*
"""
conformable_aggregators(K::Keys, aggregators::NamedTuple) = NamedTuple{K}(aggregators)

conformable_aggregators(K::Keys, aggregators::AbstractDict) =
    NamedTuple{K}(getindex(aggregators, k) for k in K)

"""
$(SIGNATURES)

Aggregate columns.

`aggregators` is either a `NamedTuple` or an `AbstractDict` which maps column names to
functions.

*Hint*: use a `DataStructures.DefaultDict` or similar to avoid specifying all column names.
"""
aggregate(ft::FunctionalTable, aggregators) =
    map((f, c) -> f(c), conformable_aggregators(keys(ft), aggregators), ft.columns)

aggregate(gt::GroupedTable, aggregators) =
    GroupedTable(gt.grouping, FunctionalTable(map(c -> [c], aggregate(gt.ft, aggregators))))

function aggregate(g::GroupedBlocks{K}, aggregators) where K
    @unpack ft, cfg = g
    a = conformable_aggregators(dropkeys(keys(ft), K), aggregators)
    FunctionalTable(Iterators.flatten(imap(gb -> FunctionalTable(aggregate(gb, a)), g));
                    sorting = getsorting(g), cfg = cfg)
end
