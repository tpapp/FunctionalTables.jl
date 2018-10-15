"""
Type for keys, used internally.
"""
const Keys = Tuple{Vararg{Symbol}}

"""
$(SIGNATURES)

Test if a collection of element type `T` can contain a new element `elt` without *any* loss
of precision.
"""
@inline cancontain(T, elt::S) where {S} = S <: T || T ≡ promote_type(S, T)

@inline cancontain(T::Type{<:Integer}, elt::Integer) where {S <: Integer} =
    typemin(T) ≤ elt ≤ typemax(T)

@inline cancontain(T::Type{<:AbstractFloat}, elt::Integer) =
    (m = Integer(maxintfloat(T)); -m ≤ elt ≤ m)

"""
$(SIGNATURES)

Convert the argument to a narrower type if possible without losing precision.

!!! note
    This function is not type stable, use only when new container types are determined.
"""
@inline narrow(x) = x

@inline function narrow(x::Integer)
    intype(T) = typemin(T) ≤ x ≤ typemax(T)
    if intype(Bool)
        Bool(x)
    elseif intype(Int8)
        Int8(x)
    elseif intype(Int16)
        Int16(x)
    elseif intype(Int32)
        Int32(x)
    elseif intype(Int64)
        Int64(x)
    else
        x
    end
end

"""
$(SIGNATURES)

Append `elt` to `v`, allocating a new vector and copying the contents.

Type of new collection is calculated using `promote_type`.
"""
function append1(v::Vector{T}, elt::S) where {T,S}
    U = promote_type(T, S)
    w = Vector{U}(undef, length(v) + 1)
    copyto!(w, v)
    w[end] = elt
    w
end

struct NamedTupleSplitter{K <: NamedTuple} end

"""
$(SIGNATURES)

A callable that splits a named tuple into the fields with `names` and the rest.

Returns two `NamedTuple`s; the first one is ordered as `names`, the second one as in the
original argument.
```jldoctest
julia> s = NamedTupleSplitter((:a, :c));

julia> s((c = 1, b = 2, a = 3, d = 4))
((a = 3, c = 1), (b = 2, d = 4))
```
"""
NamedTupleSplitter(names::Keys) = NamedTupleSplitter{NamedTuple{names}}()

@inline (::NamedTupleSplitter{K})(nt::NamedTuple) where K = K(nt), Base.structdiff(nt, K)
