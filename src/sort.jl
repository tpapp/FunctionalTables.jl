####
#### Sort FunctionalTable by rows.
####

function sort_in_memory(ft::FunctionalTable, cs::ColumnSorting)
    FunctionalTable(sort!(collect(ft), lt = (a, b) -> isless_sorting(cs, a, b)),
                    cs, SORTING_TRUST)
end

function Base.sort(ft::FunctionalTable, sortspecs)
    sorting = column_sorting(sortspecs, keys(ft))
    # TODO select appropriate sorting method using heuristics for table size & sorting
    sort_in_memory(ft, sorting)
end
