#####
##### Utilities
#####

####
#### Manipulating keys (tuples of Symbol)
####

"""
Type for keys, used internally.
"""
const Keys = Tuple{Vararg{Symbol}}

"""
$(SIGNATURES)

Check that `argkeys` are a subset of the valid keys.

When that does not hold, throw and error with an informative message.
"""
function checkvalidkeys(argkeys::Keys, validkeys::Keys)
    for key in argkeys
        @argcheck key ∈ validkeys "Invalid key $(key) ∉ $(validkeys)."
    end
    nothing
end

"""
$(SIGNATURES)

Check that `drop ⊆ ftkeys`, then return `ftkeys ∖ drop`.
"""
function dropkeys(ftkeys::Keys, drop::Keys)
    checkvalidkeys(drop, ftkeys)
    tuple(setdiff(ftkeys, drop)...)
end

# FIXME: is keys_following and is_ordered_subset used anywhere?
# If not, remove.

"""
$(SIGNATURES)

If `rest` contains `key`, return the tail starting with `key`, otherwise `()`.
"""
keys_following(key::Symbol, rest::Keys) = _keys_following(key, rest...)

_keys_following(key) = ()

_keys_following(key, rest...) =
    key ≡ first(rest) ? rest : _keys_following(key, Base.tail(rest)...)

"""
$(SIGNATURES)

Test if `a ⊆ b` and the elements of `a` have the same order in `b`.
"""
is_ordered_subset(a::Keys, b::Keys) = _is_ordered_subset(b, a...)

_is_ordered_subset(b) = true

function _is_ordered_subset(b, a_first, a_rest...)
    following = keys_following(a_first, b)
    isempty(following) ? false : _is_ordered_subset(Base.tail(following), a_rest...)
end

"""
$(SIGNATURES)

Test if `b` starts with `a`.
"""
is_prefix(a, b) = length(a) ≤ length(b) && all(a == b for (a,b) in zip(a, b))

####
#### Container element type management
####

"""
$(SIGNATURES)

Test if a collection of element type `T` can contain a new element `elt` without *any* loss
of precision.
"""
@inline cancontain(T, elt::S) where {S} = S <: T || T ≡ promote_type(S, T)

@inline cancontain(::Type{Union{}}, _) = false

@inline cancontain(::Type{Union{}}, ::Integer) = false

@inline cancontain(T::Type{<:Integer}, elt::Integer) where {S <: Integer} =
    typemin(T) ≤ elt ≤ typemax(T)

@inline cancontain(T::Type{<:AbstractFloat}, elt::Integer) =
    (m = Integer(maxintfloat(T)); -m ≤ elt ≤ m)

"""
$(SIGNATURES)

Convert the argument to a narrower type if possible without losing precision.

!!! NOTE
    This function is not type stable, use only when new container types are determined.
"""
@inline narrow(x) = x

@inline narrow(x::Bool) = x

@inline function narrow(x::Integer)
    intype(T) = typemin(T) ≤ x ≤ typemax(T)
    if intype(Int8)
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

####
#### Miscellaneous
####

"""
$(SIGNATURES)

Splits a named tuple in two, based on the names in `splitter`.

Returns two `NamedTuple`s; the first one is ordered as `splitter`, the second one with the
remaining values as in the original argument.

```jldoctest
julia> split_namedtuple(NamedTuple{(:a, :c)}, (c = 1, b = 2, a = 3, d = 4))
((a = 3, c = 1), (b = 2, d = 4))
```
"""
@inline split_namedtuple(splitter::Type{<:NamedTuple}, nt::NamedTuple) =
    splitter(nt), Base.structdiff(nt, splitter)
