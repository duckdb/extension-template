# Duck_Geoarrow


This extension, Duck_Geoarrow, provides functions to convert between WKB (Well-Known Binary) geometry and the [GeoArrow](https://geoarrow.org/) coordinate encoding, powered by the [geoarrow-c](https://github.com/geoarrow/geoarrow-c) library.

## Functions

### `duck_geoarrow_version`

**Signature**
```
VARCHAR duck_geoarrow_version()
```

**Description**

Returns the version string of the loaded duck_geoarrow extension together with the version of the bundled geoarrow-c library.

**Example**
```sql
SELECT duck_geoarrow_version();
```
```
┌─────────────────────────────────────┐
│       duck_geoarrow_version()       │
│               varchar               │
├─────────────────────────────────────┤
│ 3909d51 (geoarrow-c 0.2.0-SNAPSHOT) │
└─────────────────────────────────────┘
```

### `st_asgeoarrow`

**Signature**
```
STRUCT(...) st_asgeoarrow(geom GEOMETRY)
STRUCT(...) st_asgeoarrow(wkb BLOB)
```

**Description**

Converts a DuckDB GEOMETRY (or WKB BLOB) into a GeoArrow struct with separated coordinate arrays. The output struct contains:

| Field | Type | Description |
|-------|------|-------------|
| `geometry_type` | `UTINYINT` | Geometry type (1=Point, 2=LineString, 3=Polygon, 4=MultiPoint, 5=MultiLineString, 6=MultiPolygon) |
| `xs` | `DOUBLE[]` | All X coordinates |
| `ys` | `DOUBLE[]` | All Y coordinates |
| `ring_offsets` | `INTEGER[]` | Cumulative coordinate count at the end of each ring/part |
| `geom_offsets` | `INTEGER[]` | Cumulative ring count at the end of each sub-geometry (MultiPolygon only) |

**Examples**

```sql
SELECT st_asgeoarrow('POINT(30 10)'::GEOMETRY);
```
```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     st_asgeoarrow(CAST('POINT(30 10)' AS GEOMETRY))            │
├─────────────────────────────────────────────────────────────────────────────────┤
│ {'geometry_type': 1, 'xs': [30.0], 'ys': [10.0], 'ring_offsets': [],          │
│  'geom_offsets': []}                                                           │
└─────────────────────────────────────────────────────────────────────────────────┘
```

Access individual fields:
```sql
SELECT (st_asgeoarrow('POINT(30 10)'::GEOMETRY)).xs;
-- [30.0]

SELECT (st_asgeoarrow('LINESTRING(0 0, 1 1, 2 2)'::GEOMETRY)).geometry_type;
-- 2
```

### `st_geomfromgeoarrow`

**Signature**
```
GEOMETRY st_geomfromgeoarrow(
    geom STRUCT(geometry_type UTINYINT, xs DOUBLE[], ys DOUBLE[],
                ring_offsets INTEGER[], geom_offsets INTEGER[])
)
```

**Description**

Converts a GeoArrow struct (as produced by `st_asgeoarrow`) back into a DuckDB GEOMETRY. Supports Point, LineString, Polygon, MultiPoint, MultiLineString, and MultiPolygon geometry types.

**Examples**

Construct a LineString from coordinates:
```sql
SELECT st_geomfromgeoarrow({
    'geometry_type': 2::UTINYINT,
    'xs': [0.0, 1.0, 2.0],
    'ys': [0.0, 1.0, 2.0],
    'ring_offsets': []::INTEGER[],
    'geom_offsets': []::INTEGER[]
});
```
```
┌──────────────────────────────┐
│   st_geomfromgeoarrow(...)   │
│           geometry           │
├──────────────────────────────┤
│ LINESTRING (0 0, 1 1, 2 2)  │
└──────────────────────────────┘
```

Roundtrip GEOMETRY through GeoArrow:
```sql
SELECT st_geomfromgeoarrow(st_asgeoarrow(geom_column)) FROM my_table;
```

## Building

### Build steps
To build the extension, run:
```sh
make
```

This uses ninja by default for faster builds. The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/duck_geoarrow/duck_geoarrow.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `duck_geoarrow.duckdb_extension` is the loadable binary as it would be distributed.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL duck_geoarrow;
LOAD duck_geoarrow;
```
