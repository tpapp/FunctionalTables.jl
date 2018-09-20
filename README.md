# DataColumns

![Lifecycle](https://img.shields.io/badge/lifecycle-experimental-orange.svg)<!--
![Lifecycle](https://img.shields.io/badge/lifecycle-maturing-blue.svg)
![Lifecycle](https://img.shields.io/badge/lifecycle-stable-green.svg)
![Lifecycle](https://img.shields.io/badge/lifecycle-retired-orange.svg)
![Lifecycle](https://img.shields.io/badge/lifecycle-archived-red.svg)
![Lifecycle](https://img.shields.io/badge/lifecycle-dormant-blue.svg) -->
[![Build Status](https://travis-ci.org/tpapp/DataColumns.jl.svg?branch=master)](https://travis-ci.org/tpapp/DataColumns.jl)
[![Coverage Status](https://coveralls.io/repos/tpapp/DataColumns.jl/badge.svg?branch=master&service=github)](https://coveralls.io/github/tpapp/DataColumns.jl?branch=master)
[![codecov.io](http://codecov.io/github/tpapp/DataColumns.jl/coverage.svg?branch=master)](http://codecov.io/github/tpapp/DataColumns.jl?branch=master)



## API and architecture notes

### Column storage and element access

- Columns are immutable.

- Columns are created with the following interface:

1. A `sink` is initialized. It has op

```julia
store!(column, elt)
```

will return `true` if `column` could save `elt`, `false` if it couldn't. In the latter case,

```julia
newcolumn = extend(column, elt)
```
should be used to allocate a new column that
