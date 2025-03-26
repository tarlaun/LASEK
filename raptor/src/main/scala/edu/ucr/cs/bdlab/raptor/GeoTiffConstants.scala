package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants

/**
 * Constants associated with GeoTiff.
 */
object GeoTiffConstants {
  /**
   * Type = DOUBLE (IEEE Double precision)
   * N = 3
   * This tag may be used to specify the size of raster pixel spacing in the
   * model space units, when the raster space can be embedded in the model
   * space coordinate system without rotation, and consists of the following 3 values:
   * ModelPixelScaleTag = (ScaleX, ScaleY, ScaleZ)
   * where ScaleX and ScaleY give the horizontal and vertical spacing of
   * raster pixels. The ScaleZ is primarily used to map the pixel value of a
   * digital elevation model into the correct Z-scale, and so for most other
   * purposes this value should be zero (since most model spaces are 2-D, with Z=0).
   * For more details, check the GeoTIFF specs page 26.
   */
  val ModelPixelScaleTag: Short = 33550.toShort

  /**
   * Type = DOUBLE (IEEE Double precision)
   * N = 6*K, K = number of tiepoints
   * Alias: GeoreferenceTag
   * This tag stores raster-&gt;model tiepoint pairs in the order
   * ModelTiepointTag = (...,I,J,K, X,Y,Z...),
   */
  val ModelTiepointTag: Short = 33922.toShort

  /**
   * This tag may be used to store the GeoKey Directory, which defines and references
   * the "GeoKeys", as described below.
   * The tag is an array of unsigned SHORT values, which are primarily grouped into
   * blocks of 4. The first 4 values are special, and contain GeoKey directory header
   * information. The header values consist of the following information, in order:
   *  Header={KeyDirectoryVersion, KeyRevision, MinorRevision, NumberOfKeys}
   * where:
   *  - "KeyDirectoryVersion" indicates the current version of Key
   * implementation, and will only change if this Tag's Key
   * structure is changed. (Similar to the TIFFVersion (42)).
   * The current DirectoryVersion number is 1. This value will
   * most likely never change, and may be used to ensure that
   * this is a valid Key-implementation.
   *  - "KeyRevision" indicates what revision of Key-Sets are used.
   *  - "MinorRevision" indicates what set of Key-codes are used. The
   * complete revision number is denoted &lt;KeyRevision&gt;.&lt;MinorRevision&gt;
   *  - "NumberOfKeys" indicates how many Keys are defined by the rest of this Tag.
   *
   * This header is immediately followed by a collection of &lt;NumberOfKeys&gt; KeyEntry
   * sets, each of which is also 4-SHORTS long. Each KeyEntry is modeled on the
   * "TIFFEntry" format of the TIFF directory header, and is of the form:
   * KeyEntry = { KeyID, TIFFTagLocation, Count, Value_Offset }
   * where
   *    - "KeyID" gives the key-ID value of the Key (identical in function
   * to TIFF tag ID, but completely independent of TIFF tag-space),
   *    - "TIFFTagLocation" indicates which TIFF tag contains the value(s)
   * of the Key: if TIFFTagLocation is 0, then the value is SHORT,
   * and is contained in the "Value_Offset" entry. Otherwise, the type
   * (format) of the value is implied by the TIFF-Type of the tag
   * containing the value.
   *    - "Count" indicates the number of values in this key.
   *    - "Value_Offset" Value_Offset indicates the index-
   * offset *into* the TagArray indicated by TIFFTagLocation, if
   * it is nonzero. If TIFFTagLocation=0, then Value_Offset
   * contains the actual (SHORT) value of the Key, and
   * Count=1 is implied. Note that the offset is not a byte-offset,
   * but rather an index based on the natural data type of the
   * specified tag array.
   *
   * Following the KeyEntry definitions, the KeyDirectory tag may also contain
   * additional values. For example, if a Key requires multiple SHORT values, they
   * shall be placed at the end of this tag, and the KeyEntry will set
   * TIFFTagLocation=GeoKeyDirectoryTag, with the Value_Offset pointing to the
   * location of the value(s).
   *
   * All key-values which are not of type SHORT are to be stored in one of the following
   * two tags, based on their format:
   */
  val GeoKeyDirectoryTag: Short = 34735.toShort

  /**
   * Tag = 34736 (87BO.H)
   * Type = DOUBLE (IEEE Double precision)
   * N = variable
   * Owner: SPOT Image, Inc.
   * This tag is used to store all of the DOUBLE valued GeoKeys, referenced by the
   * [[GeoKeyDirectoryTag]]. The meaning of any value of this double array is determined
   * from the GeoKeyDirectoryTag reference pointing to it.
   * FLOAT values should first be converted to DOUBLE and stored here.
   */
  val GeoDoubleParamsTag: Short = 34736.toShort

  /**
   * This tag is used to store all of the ASCII valued GeoKeys, referenced by the
   * GeoKeyDirectoryTag. Since keys use offsets into tags, any special comments
   * may be placed at the beginning of this tag. For the most part, the only keys
   * that are ASCII valued are "Citation" keys, giving documentation and references
   * for obscure projections, datums, etc.
   * Note on ASCII Keys:
   * Special handling is required for ASCII-valued keys. While it is true that TIFF
   * 6.0 permits multiple NULL-delimited strings within a single ASCII tag, the
   * secondary strings might not appear in the output of naive "tiffdump" programs.
   * For this reason, the null delimiter of each ASCII Key value shall be converted
   * to a "|" (pipe) character before being installed back into the ASCII holding
   * tag, so that a dump of the tag will look like this.
   * AsciiTag="first_value|second_value|etc...last_value|"
   * A baseline GeoTIFF-reader must check for and convert the final "|" pipe character
   * of a key back into a NULL before returning it to the client software.
   * In the TIFF spec it is required that TIFF tags be written out to the file in
   * tag-ID sorted order. This is done to avoid forcing software to perform N-squared
   * sort operations when reading and writing tags.
   * To follow the TIFF philosophy, GeoTIFF-writers shall store the GeoKey entries
   * in key-sorted order within the CoordSystemInfoTag.
   * Example:
   * GeoKeyDirectoryTag=( 1, 1, 2, 6,
   * 1024, 0, 1, 2,
   * 1026, 34737,12, 0,
   * 2048, 0, 1, 32767,
   * 2049, 34737,14, 12,
   * 2050, 0, 1, 6,
   * 2051, 34736, 1, 0 )
   * GeoDoubleParamsTag(34736)=(1.5)
   * GeoAsciiParamsTag(34737)=("Custom File|My Geographic|")
   * The first line indicates that this is a Version 1 GeoTIFF GeoKey directory,
   * the keys are Rev. 1.2, and there are 6 Keys defined in this tag.
   * The next line indicates that the first Key (ID=1024 = GTModelTypeGeoKey) has
   * the value 2 (Geographic), explicitly placed in the entry list (since
   * TIFFTagLocation=0). The next line indicates that the Key 1026 (the
   * GTCitationGeoKey) is listed in the GeoAsciiParamsTag (34737) array, starting
   * at offset 0 (the first in array), and running for 12 bytes and so has the value
   * "Custom File" (the "|" is converted to a null delimiter at the end). Going
   * further down the list, the Key 2051 (GeogLinearUnitSizeGeoKey) is located in
   * the GeoDoubleParamsTag (34736), at offset 0 and has the value 1.5; the value
   * of key 2049 (GeogCitationGeoKey) is "My Geographic".
   * The TIFF layer handles all the problems of data structure, platform independence,
   * format types, etc, by specifying byte-offsets, byte-order format and count,
   * while the Key describes its key values at the TIFF level by specifying Tag
   * number, array-index, and count. Since all TIFF information occurs in TIFF arrays
   * of some sort, we have a robust method for storing anything in a Key that would
   * occur in a Tag.
   * With this Key-value approach, there are 65536 Keys which have all the flexibility
   * of TIFF tag, with the added advantage that a TIFF dump will provide all the
   * information that exists in the GeoTIFF implementation.
   * This GeoKey mechanism will be used extensively in section 2.7, where the numerous
   * parameters for defining Coordinate Systems and their underlying projections
   * are defined.
   */
  val GeoAsciiParamsTag: Short = 34737.toShort

  /**
   * Used by the GDAL library, holds an XML list of name=value 'metadata' values about the image as a whole,
   * and about specific samples.
   */
  val GDALMetadataTag: Short = 42112.toShort

  /**
   * Used by the GDAL library, contains an ASCII encoded nodata or background pixel value.
   */
  val GDALNoDataTag: Short = 42113.toShort

  /** Define user-friendly names for additional TIFF tags for printing out the results */
  def initializeTagNames(): Unit = {
    TiffConstants.TagNames.put(ModelPixelScaleTag, "Model Pixel Scale")
    TiffConstants.TagNames.put(ModelTiepointTag, "Model TiePoint")
    TiffConstants.TagNames.put(GeoKeyDirectoryTag, "Geokey Directory")
    TiffConstants.TagNames.put(GeoDoubleParamsTag, "Geo Double Parameters")
    TiffConstants.TagNames.put(GeoAsciiParamsTag, "Geo ASCII Parameters")
    TiffConstants.TagNames.put(GDALMetadataTag, "GDAL Metadata")
    TiffConstants.TagNames.put(GDALNoDataTag, "GDAL NoData")
  }

  /**
   * Key ID = 1024
   * Type: SHORT (code)
   * Values: Section 6.3.1.1 Codes [[ModelTypeCodes]]
   * This GeoKey defines the general type of model Coordinate system used,
   * and to which the raster space will be transformed: unknown, Geocentric (rarely used), Geographic,
   * Projected Coordinate System, or user-defined. If the coordinate system is a PCS,
   * then only the PCS code need be specified. If the coordinate system does not fit into
   * one of the standard registered PCS'S, but it uses one of the standard projections and datums,
   * then its should be documented as a PCS model with "user-defined" type, requiring
   * the specification of projection parameters, etc.
   */
  val GTModelTypeGeoKey: Short = 1024

  /**
   * Key ID = 1025
   * Type = Section 6.3.1.2 codes [[RasterTypeCodes]]
   * This establishes the Raster Space coordinate system used;
   * there are currently only two, namely [[RasterTypeCodes.RasterPixelIsPoint]]
   * and [[RasterTypeCodes.RasterPixelIsArea]].
   * No user-defined raster spaces are currently supported. For variance in imaging display parameters,
   * such as pixel aspect-ratios, use the standard TIFF 6.0 device-space tags instead.
   */
  val GTRasterTypeGeoKey: Short = 1025

  /**
   * Key ID = 1026
   * Type = ASCII
   * As with all the "Citation" GeoKeys, this is provided to give an ASCII reference
   * to published documentation on the overall configuration of this GeoTIFF file.
   */
  val GTCitationGeoKey: Short = 1026

  /**
   * GeoTIFF specs Section 6.2.2
   * GeoKey parameters for geographic coordinate system
   */
  object GeographicCSParameterKeys {
    /**
     * Key ID = 2048
     * Type = SHORT (code)
     * Values = Section 6.3.2.1 Codes [[GeographicCSTypeCodes]]
     * This key may be used to specify the code for the geographic coordinate
     * system used to map lat-long to a specific ellipsoid over the earth.
     */
    val GeographicTypeGeoKey: Short = 2048

    /**
     * Key ID = 2049
     * Type = ASCII
     * Values = text
     * General citation and reference for all Geographic CS parameters.
     */
    val GeogCitationGeoKey: Short = 2049

    /**
     * Key ID = 2050
     * Type = SHORT (code)
     * Values = Section 6.3.2.2 Codes [[GeodeticDatumCodes]]
     * This key may be used to specify the horizontal datum, defining the size, position and orientation
     * of the reference ellipsoid used in user-defined geographic coordinate systems.
     */
    val GeogGeodeticDatumGeoKey: Short = 2050

    /**
     * Key ID = 2051
     * Type = SHORT (code)
     * Units: Section 6.3.2.4 code [[PrimeMeridianCodes]]
     * Allows specification of the location of the Prime meridian for user-defined geographic coordinate systems.
     * The default standard is Greenwich, England.
     */
    val GeogPrimeMeridianGeoKey: Short = 2051

    /**
     * Key ID = 2052
     * Type = DOUBLE
     * Values: Section 6.3.1.3 Codes [[LinearUnitsCodes]]
     * Allows the definition of geocentric CS linear units for user-defined GCS.
     */
    val GeogLinearUnitsGeoKey: Short = 2052
    /**Key ID = 2053
     * Type = DOUBLE
     * Units: meters
     * Allows the definition of user-defined linear geocentric units, as measured in meters.
     */
    val GeogLinearUnitSizeGeoKey: Short = 2053
    /**
     * Key ID = 2054
     * Type = SHORT (code)
     * Values = Section 6.3.1.4 Codes [[AngularUnitsCodes]]
     * Allows the definition of geocentric CS Linear units for user-defined GCS and for ellipsoids.
     */
    val GeogAngularUnitsGeoKey: Short = 2054
    /** Key ID = 2055
     * Type = DOUBLE
     * Units: radians
     * Allows the definition of user-defined angular geographic units, as measured in radians.
     */
    val GeogAngularUnitSizeGeoKey: Short = 2055
    /**
     * Key ID = 2056
     * Type = SHORT (code)
     * Values = Section 6.3.2.3 Codes [[EllipsoidCodes]]
     * This key may be used to specify the coded ellipsoid used in the
     * geodetic datum of the Geographic Coordinate System.
     */
    val GeogEllipsoidGeoKey: Short = 2056
    /** GeogLinearUnits */
    val GeogSemiMajorAxisGeoKey: Short = 2057
    /** GeogLinearUnits */
    val GeogSemiMinorAxisGeoKey: Short = 2058
    /** ratio */
    val GeogInvFlatteningGeoKey: Short = 2059
    /** Section 6.3.1.4 Codes [[AngularUnitsCodes]] */
    val GeogAzimuthUnitsGeoKey: Short = 2060
    /**
     * Key ID = 2061
     * Type = DOUBLE
     * Units = GeogAngularUnits
     * This key allows definition of user-defined Prime Meridians, the location
     * of which is defined by its longitude relative to Greenwich.
     */
    val GeogPrimeMeridianLongGeoKey: Short = 2061
  }

  /**
   * Parameter keys for projected coordinate system from GeoTIFF specs Section 6.2.3
   */
  object ProjectedCSParameterKeys {
    /** Section 6.3.3.1 codes */
    val ProjectedCSTypeGeoKey: Short = 3072
    /** documentation */
    val PCSCitationGeoKey: Short = 3073
    /** Section 6.3.3.2 codes */
    val ProjectionGeoKey: Short = 3074
    /** Section 6.3.3.3 codes */
    val ProjCoordTransGeoKey: Short = 3075
    /** Section 6.3.1.3 codes  */
    val ProjLinearUnitsGeoKey: Short = 3076
    /** meters */
    val ProjLinearUnitSizeGeoKey: Short = 3077
    /** GeogAngularUnit */
    val ProjStdParallel1GeoKey: Short = 3078
    /** GeogAngularUnit */
    val ProjStdParallel2GeoKey: Short = 3079
    /** GeogAngularUnit */
    val ProjNatOriginLongGeoKey: Short = 3080
    /** GeogAngularUnit */
    val ProjNatOriginLatGeoKey: Short = 3081
    /** ProjLinearUnits */
    val ProjFalseEastingGeoKey: Short = 3082
    /** ProjLinearUnits */
    val ProjFalseNorthingGeoKey: Short = 3083
    /** GeogAngularUnit */
    val ProjFalseOriginLongGeoKey: Short = 3084
    /** GeogAngularUnit */
    val ProjFalseOriginLatGeoKey: Short = 3085
    /** ProjLinearUnits */
    val ProjFalseOriginEastingGeoKey: Short = 3086
    /** ProjLinearUnits */
    val ProjFalseOriginNorthingGeoKey: Short = 3087
    /** GeogAngularUnit */
    val ProjCenterLongGeoKey: Short = 3088
    /** GeogAngularUnit */
    val ProjCenterLatGeoKey: Short = 3089
    /** ProjLinearUnits */
    val ProjCenterEastingGeoKey: Short = 3090
    /** ProjLinearUnits */
    val ProjCenterNorthingGeoKey: Short = 3091
    /** ratio */
    val ProjScaleAtNatOriginGeoKey: Short = 3092
    /** ratio */
    val ProjScaleAtCenterGeoKey: Short = 3093
    /** GeogAzimuthUnit */
    val ProjAzimuthAngleGeoKey: Short = 3094
    /** GeogAngularUnit */
    val ProjStraightVertPoleLongGeoKey: Short = 3095
  }

  /**
   * GeoKey parameters for vertical coordinate system from GeoTIFF specs Section 6.2.4
   */
  object VerticalCSKeys {
    /** Section 6.3.4.1 codes */
    val VerticalCSTypeGeoKey: Short = 4096
    /** documentation */
    val VerticalCitationGeoKey: Short = 4097
    /** Section 6.3.4.2 codes */
    val VerticalDatumGeoKey: Short = 4098
    /** Section 6.3.1.3 codes */
    val VerticalUnitsGeoKey: Short = 4099
  }

  /**
   * From GeoTIFF specs Section 6.3.1.1
   */
  object ModelTypeCodes {
    /**Projection Coordinate System*/
    val ModelTypeProjected: Short = 1

    /**Geographic latitude-longitude System */
    val ModelTypeGeographic: Short = 2

    /** Geocentric (X,Y,Z) Coordinate System */
    val ModelTypeGeocentric: Short = 3
  }

  /**
   * From GeoTIFF specs Section 6.3.1.2.
   */
  object RasterTypeCodes {
    /**Indicates that each raster pixel represents a square*/
    val RasterPixelIsArea: Short = 1

    /**Indicates that each raster pixel represent a point*/
    val RasterPixelIsPoint: Short = 2
  }

  /**
   * From GeoTIFF specs Section 6.3.1.3
   * There are several different kinds of units that may be used in
   * geographically related raster data: linear units, angular units, units
   * of time (e.g. for radar-return), CCD-voltages, etc. For this reason
   * there will be a single, unique range for each kind of unit, broken down
   * into the following currently defined ranges:
   */
  object LinearUnitsCodes {
    val Linear_Meter: Short = 9001
    val Linear_Foot: Short = 9002
    val Linear_Foot_US_Survey: Short = 9003
    val Linear_Foot_Modified_American: Short = 9004
    val Linear_Foot_Clarke: Short = 9005
    val Linear_Foot_Indian: Short = 9006
    val Linear_Link: Short = 9007
    val Linear_Link_Benoit: Short = 9008
    val Linear_Link_Sears: Short = 9009
    val Linear_Chain_Benoit: Short = 9010
    val Linear_Chain_Sears: Short = 9011
    val Linear_Yard_Sears: Short = 9012
    val Linear_Yard_Indian: Short = 9013
    val Linear_Fathom: Short = 9014
    val Linear_Mile_International_Nautical: Short = 9015
  }

  /**
   * From GeoTIFF specs Section 6.3.1.4
   * These codes shall be used for any key that requires specification of an angular unit of measurement.
   */
  object AngularUnitsCodes {
    val Angular_Radian: Short = 9101
    val Angular_Degree: Short = 9102
    val Angular_Arc_Minute: Short = 9103
    val Angular_Arc_Second: Short = 9104
    val Angular_Grad: Short = 9105
    val Angular_Gon: Short = 9106
    val Angular_DMS: Short = 9107
    val Angular_DMS_Hemisphere: Short = 9108
  }

  /**
   * From GeoTIFF specs section 6.3.2.1
   * Note: A Geographic coordinate system consists of both a datum and a
   * Prime Meridian. Some of the names are very similar, and differ only in
   * the Prime Meridian, so be sure to use the correct one. The codes
   * beginning with GCSE_xxx are unspecified GCS which use ellipsoid (xxx);
   * it is recommended that only the codes beginning with GCS_ be used if possible.
   * Note: Geodetic datum using Greenwich PM have codes equal to the corresponding Datum code - 2000.
   */
  object GeographicCSTypeCodes {
    val GCS_Adindan: Short = 4201
    val GCS_AGD66: Short = 4202
    val GCS_AGD84: Short = 4203
    val GCS_Ain_el_Abd: Short = 4204
    val GCS_Afgooye: Short = 4205
    val GCS_Agadez: Short = 4206
    val GCS_Lisbon: Short = 4207
    val GCS_Aratu: Short = 4208
    val GCS_Arc_1950: Short = 4209
    val GCS_Arc_1960: Short = 4210
    val GCS_Batavia: Short = 4211
    val GCS_Barbados: Short = 4212
    val GCS_Beduaram: Short = 4213
    val GCS_Beijing_1954: Short = 4214
    val GCS_Belge_1950: Short = 4215
    val GCS_Bermuda_1957: Short = 4216
    val GCS_Bern_1898: Short = 4217
    val GCS_Bogota: Short = 4218
    val GCS_Bukit_Rimpah: Short = 4219
    val GCS_Camacupa: Short = 4220
    val GCS_Campo_Inchauspe: Short = 4221
    val GCS_Cape: Short = 4222
    val GCS_Carthage: Short = 4223
    val GCS_Chua: Short = 4224
    val GCS_Corrego_Alegre: Short = 4225
    val GCS_Cote_d_Ivoire: Short = 4226
    val GCS_Deir_ez_Zor: Short = 4227
    val GCS_Douala: Short = 4228
    val GCS_Egypt_1907: Short = 4229
    val GCS_ED50: Short = 4230
    val GCS_ED87: Short = 4231
    val GCS_Fahud: Short = 4232
    val GCS_Gandajika_1970: Short = 4233
    val GCS_Garoua: Short = 4234
    val GCS_Guyane_Francaise: Short = 4235
    val GCS_Hu_Tzu_Shan: Short = 4236
    val GCS_HD72: Short = 4237
    val GCS_ID74: Short = 4238
    val GCS_Indian_1954: Short = 4239
    val GCS_Indian_1975: Short = 4240
    val GCS_Jamaica_1875: Short = 4241
    val GCS_JAD69: Short = 4242
    val GCS_Kalianpur: Short = 4243
    val GCS_Kandawala: Short = 4244
    val GCS_Kertau: Short = 4245
    val GCS_KOC: Short = 4246
    val GCS_La_Canoa: Short = 4247
    val GCS_PSAD56: Short = 4248
    val GCS_Lake: Short = 4249
    val GCS_Leigon: Short = 4250
    val GCS_Liberia_1964: Short = 4251
    val GCS_Lome: Short = 4252
    val GCS_Luzon_1911: Short = 4253
    val GCS_Hito_XVIII_1963: Short = 4254
    val GCS_Herat_North: Short = 4255
    val GCS_Mahe_1971: Short = 4256
    val GCS_Makassar: Short = 4257
    val GCS_EUREF89: Short = 4258
    val GCS_Malongo_1987: Short = 4259
    val GCS_Manoca: Short = 4260
    val GCS_Merchich: Short = 4261
    val GCS_Massawa: Short = 4262
    val GCS_Minna: Short = 4263
    val GCS_Mhast: Short = 4264
    val GCS_Monte_Mario: Short = 4265
    val GCS_M_poraloko: Short = 4266
    val GCS_NAD27: Short = 4267
    val GCS_NAD_Michigan: Short = 4268
    val GCS_NAD83: Short = 4269
    val GCS_Nahrwan_1967: Short = 4270
    val GCS_Naparima_1972: Short = 4271
    val GCS_GD49: Short = 4272
    val GCS_NGO_1948: Short = 4273
    val GCS_Datum_73: Short = 4274
    val GCS_NTF: Short = 4275
    val GCS_NSWC_9Z_2: Short = 4276
    val GCS_OSGB_1936: Short = 4277
    val GCS_OSGB70: Short = 4278
    val GCS_OS_SN80: Short = 4279
    val GCS_Padang: Short = 4280
    val GCS_Palestine_1923: Short = 4281
    val GCS_Pointe_Noire: Short = 4282
    val GCS_GDA94: Short = 4283
    val GCS_Pulkovo_1942: Short = 4284
    val GCS_Qatar: Short = 4285
    val GCS_Qatar_1948: Short = 4286
    val GCS_Qornoq: Short = 4287
    val GCS_Loma_Quintana: Short = 4288
    val GCS_Amersfoort: Short = 4289
    val GCS_RT38: Short = 4290
    val GCS_SAD69: Short = 4291
    val GCS_Sapper_Hill_1943: Short = 4292
    val GCS_Schwarzeck: Short = 4293
    val GCS_Segora: Short = 4294
    val GCS_Serindung: Short = 4295
    val GCS_Sudan: Short = 4296
    val GCS_Tananarive: Short = 4297
    val GCS_Timbalai_1948: Short = 4298
    val GCS_TM65: Short = 4299
    val GCS_TM75: Short = 4300
    val GCS_Tokyo: Short = 4301
    val GCS_Trinidad_1903: Short = 4302
    val GCS_TC_1948: Short = 4303
    val GCS_Voirol_1875: Short = 4304
    val GCS_Voirol_Unifie: Short = 4305
    val GCS_Bern_1938: Short = 4306
    val GCS_Nord_Sahara_1959: Short = 4307
    val GCS_Stockholm_1938: Short = 4308
    val GCS_Yacare: Short = 4309
    val GCS_Yoff: Short = 4310
    val GCS_Zanderij: Short = 4311
    val GCS_MGI: Short = 4312
    val GCS_Belge_1972: Short = 4313
    val GCS_DHDN: Short = 4314
    val GCS_Conakry_1905: Short = 4315
    val GCS_WGS_72: Short = 4322
    val GCS_WGS_72BE: Short = 4324
    val GCS_WGS_84: Short = 4326
    val GCS_Bern_1898_Bern: Short = 4801
    val GCS_Bogota_Bogota: Short = 4802
    val GCS_Lisbon_Lisbon: Short = 4803
    val GCS_Makassar_Jakarta: Short = 4804
    val GCS_MGI_Ferro: Short = 4805
    val GCS_Monte_Mario_Rome: Short = 4806
    val GCS_NTF_Paris: Short = 4807
    val GCS_Padang_Jakarta: Short = 4808
    val GCS_Belge_1950_Brussels: Short = 4809
    val GCS_Tananarive_Paris: Short = 4810
    val GCS_Voirol_1875_Paris: Short = 4811
    val GCS_Voirol_Unifie_Paris: Short = 4812
    val GCS_Batavia_Jakarta: Short = 4813
    val GCS_ATF_Paris: Short = 4901
    val GCS_NDG_Paris: Short = 4902
    /*Ellipsoid-Only GCS:
      Note: the numeric code is equal to the code of the corresponding EPSG ellipsoid, minus 3000.*/
    val GCSE_Airy1830: Short = 4001
    val GCSE_AiryModified1849: Short = 4002
    val GCSE_AustralianNationalSpheroid: Short = 4003
    val GCSE_Bessel1841: Short = 4004
    val GCSE_BesselModified: Short = 4005
    val GCSE_BesselNamibia: Short = 4006
    val GCSE_Clarke1858: Short = 4007
    val GCSE_Clarke1866: Short = 4008
    val GCSE_Clarke1866Michigan: Short = 4009
    val GCSE_Clarke1880_Benoit: Short = 4010
    val GCSE_Clarke1880_IGN: Short = 4011
    val GCSE_Clarke1880_RGS: Short = 4012
    val GCSE_Clarke1880_Arc: Short = 4013
    val GCSE_Clarke1880_SGA1922: Short = 4014
    val GCSE_Everest1830_1937Adjustment: Short = 4015
    val GCSE_Everest1830_1967Definition: Short = 4016
    val GCSE_Everest1830_1975Definition: Short = 4017
    val GCSE_Everest1830Modified: Short = 4018
    val GCSE_GRS1980: Short = 4019
    val GCSE_Helmert1906: Short = 4020
    val GCSE_IndonesianNationalSpheroid: Short = 4021
    val GCSE_International1924: Short = 4022
    val GCSE_International1967: Short = 4023
    val GCSE_Krassowsky1940: Short = 4024
    val GCSE_NWL9D: Short = 4025
    val GCSE_NWL10D: Short = 4026
    val GCSE_Plessis1817: Short = 4027
    val GCSE_Struve1860: Short = 4028
    val GCSE_WarOffice: Short = 4029
    val GCSE_WGS84: Short = 4030
    val GCSE_GEM10C: Short = 4031
    val GCSE_OSU86F: Short = 4032
    val GCSE_OSU91A: Short = 4033
    val GCSE_Clarke1880: Short = 4034
    val GCSE_Sphere: Short = 4035
  }

  /**
   * Geodetic Datum Codes as defined in GeoTIFF specs Section 6.3.2.2
   */
  object GeodeticDatumCodes {
    val Datum_Adindan: Short = 6201
    val Datum_Australian_Geodetic_Datum_1966: Short = 6202
    val Datum_Australian_Geodetic_Datum_1984: Short = 6203
    val Datum_Ain_el_Abd_1970: Short = 6204
    val Datum_Afgooye: Short = 6205
    val Datum_Agadez: Short = 6206
    val Datum_Lisbon: Short = 6207
    val Datum_Aratu: Short = 6208
    val Datum_Arc_1950: Short = 6209
    val Datum_Arc_1960: Short = 6210
    val Datum_Batavia: Short = 6211
    val Datum_Barbados: Short = 6212
    val Datum_Beduaram: Short = 6213
    val Datum_Beijing_1954: Short = 6214
    val Datum_Reseau_National_Belge_1950: Short = 6215
    val Datum_Bermuda_1957: Short = 6216
    val Datum_Bern_1898: Short = 6217
    val Datum_Bogota: Short = 6218
    val Datum_Bukit_Rimpah: Short = 6219
    val Datum_Camacupa: Short = 6220
    val Datum_Campo_Inchauspe: Short = 6221
    val Datum_Cape: Short = 6222
    val Datum_Carthage: Short = 6223
    val Datum_Chua: Short = 6224
    val Datum_Corrego_Alegre: Short = 6225
    val Datum_Cote_d_Ivoire: Short = 6226
    val Datum_Deir_ez_Zor: Short = 6227
    val Datum_Douala: Short = 6228
    val Datum_Egypt_1907: Short = 6229
    val Datum_European_Datum_1950: Short = 6230
    val Datum_European_Datum_1987: Short = 6231
    val Datum_Fahud: Short = 6232
    val Datum_Gandajika_1970: Short = 6233
    val Datum_Garoua: Short = 6234
    val Datum_Guyane_Francaise: Short = 6235
    val Datum_Hu_Tzu_Shan: Short = 6236
    val Datum_Hungarian_Datum_1972: Short = 6237
    val Datum_Indonesian_Datum_1974: Short = 6238
    val Datum_Indian_1954: Short = 6239
    val Datum_Indian_1975: Short = 6240
    val Datum_Jamaica_1875: Short = 6241
    val Datum_Jamaica_1969: Short = 6242
    val Datum_Kalianpur: Short = 6243
    val Datum_Kandawala: Short = 6244
    val Datum_Kertau: Short = 6245
    val Datum_Kuwait_Oil_Company: Short = 6246
    val Datum_La_Canoa: Short = 6247
    val Datum_Provisional_S_American_Datum_1956: Short = 6248
    val Datum_Lake: Short = 6249
    val Datum_Leigon: Short = 6250
    val Datum_Liberia_1964: Short = 6251
    val Datum_Lome: Short = 6252
    val Datum_Luzon_1911: Short = 6253
    val Datum_Hito_XVIII_1963: Short = 6254
    val Datum_Herat_North: Short = 6255
    val Datum_Mahe_1971: Short = 6256
    val Datum_Makassar: Short = 6257
    val Datum_European_Reference_System_1989: Short = 6258
    val Datum_Malongo_1987: Short = 6259
    val Datum_Manoca: Short = 6260
    val Datum_Merchich: Short = 6261
    val Datum_Massawa: Short = 6262
    val Datum_Minna: Short = 6263
    val Datum_Mhast: Short = 6264
    val Datum_Monte_Mario: Short = 6265
    val Datum_M_poraloko: Short = 6266
    val Datum_North_American_Datum_1927: Short = 6267
    val Datum_NAD_Michigan: Short = 6268
    val Datum_North_American_Datum_1983: Short = 6269
    val Datum_Nahrwan_1967: Short = 6270
    val Datum_Naparima_1972: Short = 6271
    val Datum_New_Zealand_Geodetic_Datum_1949: Short = 6272
    val Datum_NGO_1948: Short = 6273
    val Datum_Datum_73: Short = 6274
    val Datum_Nouvelle_Triangulation_Francaise: Short = 6275
    val Datum_NSWC_9Z_2: Short = 6276
    val Datum_OSGB_1936: Short = 6277
    val Datum_OSGB_1970_SN: Short = 6278
    val Datum_OS_SN_1980: Short = 6279
    val Datum_Padang_1884: Short = 6280
    val Datum_Palestine_1923: Short = 6281
    val Datum_Pointe_Noire: Short = 6282
    val Datum_Geocentric_Datum_of_Australia_1994: Short = 6283
    val Datum_Pulkovo_1942: Short = 6284
    val Datum_Qatar: Short = 6285
    val Datum_Qatar_1948: Short = 6286
    val Datum_Qornoq: Short = 6287
    val Datum_Loma_Quintana: Short = 6288
    val Datum_Amersfoort: Short = 6289
    val Datum_RT38: Short = 6290
    val Datum_South_American_Datum_1969: Short = 6291
    val Datum_Sapper_Hill_1943: Short = 6292
    val Datum_Schwarzeck: Short = 6293
    val Datum_Segora: Short = 6294
    val Datum_Serindung: Short = 6295
    val Datum_Sudan: Short = 6296
    val Datum_Tananarive_1925: Short = 6297
    val Datum_Timbalai_1948: Short = 6298
    val Datum_TM65: Short = 6299
    val Datum_TM75: Short = 6300
    val Datum_Tokyo: Short = 6301
    val Datum_Trinidad_1903: Short = 6302
    val Datum_Trucial_Coast_1948: Short = 6303
    val Datum_Voirol_1875: Short = 6304
    val Datum_Voirol_Unifie_1960: Short = 6305
    val Datum_Bern_1938: Short = 6306
    val Datum_Nord_Sahara_1959: Short = 6307
    val Datum_Stockholm_1938: Short = 6308
    val Datum_Yacare: Short = 6309
    val Datum_Yoff: Short = 6310
    val Datum_Zanderij: Short = 6311
    val Datum_Militar_Geographische_Institut: Short = 6312
    val Datum_Reseau_National_Belge_1972: Short = 6313
    val Datum_Deutsche_Hauptdreiecksnetz: Short = 6314
    val Datum_Conakry_1905: Short = 6315
    val Datum_WGS72: Short = 6322
    val Datum_WGS72_Transit_Broadcast_Ephemeris: Short = 6324
    val Datum_WGS84: Short = 6326
    val Datum_Ancienne_Triangulation_Francaise: Short = 6901
    val Datum_Nord_de_Guerre: Short = 6902
    /*
    Ellipsoid-Only Datum:
     Note: the numeric code is equal to the corresponding ellipsoid code, minus 1000.
     */
    val DatumE_Airy1830: Short = 6001
    val DatumE_AiryModified1849: Short = 6002
    val DatumE_AustralianNationalSpheroid: Short = 6003
    val DatumE_Bessel1841: Short = 6004
    val DatumE_BesselModified: Short = 6005
    val DatumE_BesselNamibia: Short = 6006
    val DatumE_Clarke1858: Short = 6007
    val DatumE_Clarke1866: Short = 6008
    val DatumE_Clarke1866Michigan: Short = 6009
    val DatumE_Clarke1880_Benoit: Short = 6010
    val DatumE_Clarke1880_IGN: Short = 6011
    val DatumE_Clarke1880_RGS: Short = 6012
    val DatumE_Clarke1880_Arc: Short = 6013
    val DatumE_Clarke1880_SGA1922: Short = 6014
    val DatumE_Everest1830_1937Adjustment: Short = 6015
    val DatumE_Everest1830_1967Definition: Short = 6016
    val DatumE_Everest1830_1975Definition: Short = 6017
    val DatumE_Everest1830Modified: Short = 6018
    val DatumE_GRS1980: Short = 6019
    val DatumE_Helmert1906: Short = 6020
    val DatumE_IndonesianNationalSpheroid: Short = 6021
    val DatumE_International1924: Short = 6022
    val DatumE_International1967: Short = 6023
    val DatumE_Krassowsky1960: Short = 6024
    val DatumE_NWL9D: Short = 6025
    val DatumE_NWL10D: Short = 6026
    val DatumE_Plessis1817: Short = 6027
    val DatumE_Struve1860: Short = 6028
    val DatumE_WarOffice: Short = 6029
    val DatumE_WGS84: Short = 6030
    val DatumE_GEM10C: Short = 6031
    val DatumE_OSU86F: Short = 6032
    val DatumE_OSU91A: Short = 6033
    val DatumE_Clarke1880: Short = 6034
    val DatumE_Sphere: Short = 6035
  }

  /**
   * Ellipsoid codes as defined in GeoTIFF specs Section 6.3.2.3
   */
  object EllipsoidCodes {
    val Ellipse_Airy_1830: Short = 7001
    val Ellipse_Airy_Modified_1849: Short = 7002
    val Ellipse_Australian_National_Spheroid: Short = 7003
    val Ellipse_Bessel_1841: Short = 7004
    val Ellipse_Bessel_Modified: Short = 7005
    val Ellipse_Bessel_Namibia: Short = 7006
    val Ellipse_Clarke_1858: Short = 7007
    val Ellipse_Clarke_1866: Short = 7008
    val Ellipse_Clarke_1866_Michigan: Short = 7009
    val Ellipse_Clarke_1880_Benoit: Short = 7010
    val Ellipse_Clarke_1880_IGN: Short = 7011
    val Ellipse_Clarke_1880_RGS: Short = 7012
    val Ellipse_Clarke_1880_Arc: Short = 7013
    val Ellipse_Clarke_1880_SGA_1922: Short = 7014
    val Ellipse_Everest_1830_1937_Adjustment: Short = 7015
    val Ellipse_Everest_1830_1967_Definition: Short = 7016
    val Ellipse_Everest_1830_1975_Definition: Short = 7017
    val Ellipse_Everest_1830_Modified: Short = 7018
    val Ellipse_GRS_1980: Short = 7019
    val Ellipse_Helmert_1906: Short = 7020
    val Ellipse_Indonesian_National_Spheroid: Short = 7021
    val Ellipse_International_1924: Short = 7022
    val Ellipse_International_1967: Short = 7023
    val Ellipse_Krassowsky_1940: Short = 7024
    val Ellipse_NWL_9D: Short = 7025
    val Ellipse_NWL_10D: Short = 7026
    val Ellipse_Plessis_1817: Short = 7027
    val Ellipse_Struve_1860: Short = 7028
    val Ellipse_War_Office: Short = 7029
    val Ellipse_WGS_84: Short = 7030
    val Ellipse_GEM_10C: Short = 7031
    val Ellipse_OSU86F: Short = 7032
    val Ellipse_OSU91A: Short = 7033
    val Ellipse_Clarke_1880: Short = 7034
    val Ellipse_Sphere: Short = 7035
  }

  /**
   * Prime Meridian Codes as defined in GeoTIFF specs Section 6.3.2.4
   */
  object PrimeMeridianCodes {
    val PM_Greenwich: Short = 8901
    val PM_Lisbon: Short = 8902
    val PM_Paris: Short = 8903
    val PM_Bogota: Short = 8904
    val PM_Madrid: Short = 8905
    val PM_Rome: Short = 8906
    val PM_Bern: Short = 8907
    val PM_Jakarta: Short = 8908
    val PM_Ferro: Short = 8909
    val PM_Brussels: Short = 8910
    val PM_Stockholm: Short = 8911
  }

  /**
   * Coordinate Transformation Codes as defined in Section 6.3.3.3
   */
  object CoordinateTransformationCodes {
    val CT_TransverseMercator = 1
    val CT_TransvMercator_Modified_Alaska = 2
    val CT_ObliqueMercator = 3
    val CT_ObliqueMercator_Laborde = 4
    val CT_ObliqueMercator_Rosenmund = 5
    val CT_ObliqueMercator_Spherical = 6
    val CT_Mercator = 7
    val CT_LambertConfConic_2SP = 8
    val CT_LambertConfConic_Helmert = 9
    val CT_LambertAzimEqualArea = 10
    val CT_AlbersEqualArea = 11
    val CT_AzimuthalEquidistant = 12
    val CT_EquidistantConic = 13
    val CT_Stereographic = 14
    val CT_PolarStereographic = 15
    val CT_ObliqueStereographic = 16
    val CT_Equirectangular = 17
    val CT_CassiniSoldner = 18
    val CT_Gnomonic = 19
    val CT_MillerCylindrical = 20
    val CT_Orthographic = 21
    val CT_Polyconic = 22
    val CT_Robinson = 23
    val CT_Sinusoidal = 24
    val CT_VanDerGrinten = 25
    val CT_NewZealandMapGrid = 26
    val CT_TransvMercator_SouthOriented= 27
  }
}
