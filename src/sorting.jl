"""
$(TYPEDEF)

Sort specification for a column.

Not part of the interface, for internal representation.
"""
struct ColumnSort
    key::Symbol
    rev::Bool
end

"""
$(SIGNATURES)

Process an individual sort specification, called by [`column_sorting`](@ref).
"""
ColumnSort(key::Symbol) = ColumnSort(key, false)
ColumnSort(keyrev::Pair{Symbol, typeof(reverse)}) = ColumnSort(first(keyrev), true)
ColumnSort(cs::ColumnSort) = cs
ColumnSort(x) = throw(ArgumentError("Unrecognized sorting specification $(x)."))

"""
$(SIGNATURES)

Process sorting specifications for columns.

Verify that sort keys are unique. When `keys` is given, verify that the sort keys are a
subset of it.

Accepted syntax:

- `:key`, for sorting a column in ascending order

- `:key => reverse`, for sorting a column in descending order

All functions which accept sort specs should use this, but the function itself is not part
of the API.
"""
function column_sorting(sortspecs, keys::Union{Nothing,Keys} = nothing)
    sorting = map(ColumnSort, tuple(sortspecs...))
    sortkeys = map(c -> c.key, sorting)
    @argcheck allunique(sortkeys) "Duplicate sort keys."
    keys ≡ nothing || @argcheck sortkeys ⊆ keys
    sorting
end

"""
Type for sorting, used internally.
"""
const Sorting = Tuple{Vararg{ColumnSort}}

"""
$(SIGNATURES)

Calculate sorting when a table with `sorting` is merged with a table containing `otherkeys`,
which may replace columns.
"""
function merge_sorting(sorting::Sorting, otherkeys::Keys)
    firstinvalid = findfirst(s -> s.key ∈ otherkeys, sorting)
    firstinvalid ≡ nothing ? sorting : sorting[1:(firstinvalid-1)]
end

"""
$(SIGNATURES)

Calculate sorting when only `keep` keys are kept.
"""
select_sorting(sorting::Sorting, keep::Keys) =
    tuple(filter(cs -> cs.key ∈ keep, [sorting...])...)
