####
#### Sort FunctionalTable by rows.
####

function sort_in_memory(ft::FunctionalTable, ordering::TableOrdering)
    FunctionalTable(sort!(collect(ft), lt = (a, b) -> isless_ordering(ordering, a, b)),
                    TrustOrdering(ordering))
end

function Base.sort(ft::FunctionalTable, column_ordering_specifications)
    ordering = table_ordering(column_ordering_specifications)
    # TODO select appropriate sorting method using heuristics for table size & sorting
    sort_in_memory(ft, ordering)
end
