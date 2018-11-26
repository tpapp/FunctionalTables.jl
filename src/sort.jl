####
#### Sort FunctionalTable by rows.
####

function sort_in_memory(ft::FunctionalTable, sorting::Sorting)
    FunctionalTable(sort!(collect(ft),
                          lt = (a, b) -> cmp_sorting(sorting, a, b) == -1);
                    sorting = sorting)
end

function Base.sort(ft::FunctionalTable, sortspecs)
    sorting = sorting_sortspecs(sortspecs, keys(ft))
    # TODO select appropriate sorting method using heuristics for table size & sorting
    sort_in_memory(ft, sorting)
end
