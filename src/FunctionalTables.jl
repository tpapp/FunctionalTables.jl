module FunctionalTables

using ArgCheck: @argcheck
using DocStringExtensions: SIGNATURES, TYPEDEF
using Parameters: @unpack
using IterTools: imap

using Base: Callable
import Base: length, IteratorSize, IteratorEltype, eltype, iterate, keys, show, merge, map

include("utilities.jl")
include("columns.jl")
include("sorting.jl")
include("tables.jl")

end # module
