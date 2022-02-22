module MMDeedleExtension
open Deedle
open Deedle.Frame

(*
この関数では、indexed_frameのキーがprimary_frameのkey_columnに
あるという前提。追加する列はindexed_frameのcolumn_to_be_copiedを
用いて構築する。データ型は型変数で指定する。indexed_frameをキーで
参照しても値が見つからないときはmissingValueを用いる。
*)
let importColumnFromForeignFrame<'X, 'C, 'R1, 'R2 
              when 'C: equality and 'R1: equality and 'R2: equality>
   (column_to_be_copied: 'C) (missingValue: 'X) (indexed_frame: Frame<'R2,'C>)
   (key_column: 'C) (primary_frame: Frame<'R1,'C>)   =

  let srs_references: Series<'R1, 'R2> = 
      primary_frame |> getCol key_column

  let (srs_cols_to_be_merged: Series<_,'X>) = 
    getCol column_to_be_copied indexed_frame

  let new_srs = 
    srs_references 
    |> Series.mapValues 
        (fun x -> if srs_cols_to_be_merged.ContainsKey(x) then
                    srs_cols_to_be_merged.[x]
                  else missingValue )

  addCol column_to_be_copied new_srs primary_frame

(*
二つのカラムを取り込む点だけが違う。
*)
let importTwoColumnsFromForeignFrame<'X1, 'X2, 'C, 'R1, 'R2 
              when 'C: equality and 'R1: equality and 'R2: equality>
   (column_to_be_copied1: 'C) (missingValue1: 'X1)
   (column_to_be_copied2: 'C) (missingValue2: 'X2) (indexed_frame: Frame<'R2,'C>)
   (key_column: 'C) (primary_frame: Frame<'R1,'C>)   =

  let srs_references: Series<'R1, 'R2> = 
      primary_frame |> getCol key_column

  let (srs_cols_to_be_merged1: Series<_,'X1>) = 
    getCol column_to_be_copied1 indexed_frame

  let (srs_cols_to_be_merged2: Series<_,'X2>) = 
    getCol column_to_be_copied2 indexed_frame

  let new_srs1 = 
    srs_references 
    |> Series.mapValues 
        (fun x -> if srs_cols_to_be_merged1.ContainsKey(x) then
                    srs_cols_to_be_merged1.[x]
                  else missingValue1 )
  let new_srs2 = 
    srs_references 
    |> Series.mapValues 
        (fun x -> if srs_cols_to_be_merged2.ContainsKey(x) then
                    srs_cols_to_be_merged2.[x]
                  else missingValue2 )

  primary_frame
  |> addCol column_to_be_copied1 new_srs1
  |> addCol column_to_be_copied2 new_srs2

(*
三つのカラムを取り込む点だけが違う。
*)
let importThreeColumnsFromForeignFrame<'X1, 'X2, 'X3, 'C, 'R1, 'R2 
              when 'C: equality and 'R1: equality and 'R2: equality>
   (column_to_be_copied1: 'C) (missingValue1: 'X1)
   (column_to_be_copied2: 'C) (missingValue2: 'X2) 
   (column_to_be_copied3: 'C) (missingValue3: 'X3) (indexed_frame: Frame<'R2,'C>)
   (key_column: 'C) (primary_frame: Frame<'R1,'C>)   =

  let srs_references: Series<'R1, 'R2> = 
      primary_frame |> getCol key_column

  let (srs_cols_to_be_merged1: Series<_,'X1>) = 
    getCol column_to_be_copied1 indexed_frame

  let (srs_cols_to_be_merged2: Series<_,'X2>) = 
    getCol column_to_be_copied2 indexed_frame

  let (srs_cols_to_be_merged3: Series<_,'X3>) = 
    getCol column_to_be_copied3 indexed_frame

  let new_srs1 = 
    srs_references 
    |> Series.mapValues 
        (fun x -> if srs_cols_to_be_merged1.ContainsKey(x) then
                    srs_cols_to_be_merged1.[x]
                  else missingValue1 )
  let new_srs2 = 
    srs_references 
    |> Series.mapValues 
        (fun x -> if srs_cols_to_be_merged2.ContainsKey(x) then
                    srs_cols_to_be_merged2.[x]
                  else missingValue2 )

  let new_srs3 = 
    srs_references 
    |> Series.mapValues 
        (fun x -> if srs_cols_to_be_merged3.ContainsKey(x) then
                    srs_cols_to_be_merged3.[x]
                  else missingValue3 )

  primary_frame
  |> addCol column_to_be_copied1 new_srs1
  |> addCol column_to_be_copied2 new_srs2
  |> addCol column_to_be_copied3 new_srs3