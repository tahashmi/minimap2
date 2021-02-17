#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <arrow-glib/arrow-glib.h>
#include <plasma-glib/plasma-glib.h>

#include <stdint-gcc.h>
#include <gmodule.h>

#include "kthread.h"
#include "kvec.h"
#include "kalloc.h"
#include "sdust.h"
#include "mmpriv.h"
#include "bseq.h"
#include "khash.h"


#define ASCII_START 32
#define ASCII_END 126

typedef char   gchar;

int64_t counts1,counts1_1,counts1_2,counts1_3,counts1_4,
        counts2,counts2_1,counts2_2,counts2_3,counts2_4,
        counts3,counts3_1,counts3_2,counts3_3,
	counts4,counts4_1,counts4_2,counts4_3,
	counts5,counts5_1,counts5_2,counts5_3,
	counts6,counts6_1,counts6_2,counts6_3,
	counts7,counts7_1,counts7_2,
	counts8,counts8_1,counts8_2,
	counts9,counts9_1,counts9_2,
	counts10,counts10_1,counts10_2,
	counts11,counts11_1,counts11_2,
	counts12,counts12_1,counts12_2,
	counts13,counts13_1,
	counts14,counts14_1,
	counts15,counts15_1,
	counts16,counts16_1,
	counts17,counts17_1,
	counts18,counts18_1,

	counts19,counts20,counts21,counts22,
	countsX,countsX_1,countsX_2,
	countsY,countsM;

//////////////////////////////////////////////////
GArrowArray *array_beginPos1;
GArrowArray *array_sam1;

GArrowInt32ArrayBuilder *builder_beginPos1;
GArrowStringArrayBuilder *builder_sam1;

GArrowArray *array_beginPos1_1;
GArrowArray *array_sam1_1;
GArrowInt32ArrayBuilder *builder_beginPos1_1;
GArrowStringArrayBuilder *builder_sam1_1;

GArrowArray *array_beginPos1_2;
GArrowArray *array_sam1_2;
GArrowInt32ArrayBuilder *builder_beginPos1_2;
GArrowStringArrayBuilder *builder_sam1_2;

GArrowArray *array_beginPos1_3;
GArrowArray *array_sam1_3;
GArrowInt32ArrayBuilder *builder_beginPos1_3;
GArrowStringArrayBuilder *builder_sam1_3;

GArrowArray *array_beginPos1_4;
GArrowArray *array_sam1_4;
GArrowInt32ArrayBuilder *builder_beginPos1_4;
GArrowStringArrayBuilder *builder_sam1_4;

////////////////////////////////////////////////
GArrowArray *array_beginPos2;
GArrowArray *array_sam2;
GArrowInt32ArrayBuilder *builder_beginPos2;
GArrowStringArrayBuilder *builder_sam2;

GArrowArray *array_beginPos2_1;
GArrowArray *array_sam2_1;
GArrowInt32ArrayBuilder *builder_beginPos2_1;
GArrowStringArrayBuilder *builder_sam2_1;

GArrowArray *array_beginPos2_2;
GArrowArray *array_sam2_2;
GArrowInt32ArrayBuilder *builder_beginPos2_2;
GArrowStringArrayBuilder *builder_sam2_2;

GArrowArray *array_beginPos2_3;
GArrowArray *array_sam2_3;
GArrowInt32ArrayBuilder *builder_beginPos2_3;
GArrowStringArrayBuilder *builder_sam2_3;

GArrowArray *array_beginPos2_4;
GArrowArray *array_sam2_4;
GArrowInt32ArrayBuilder *builder_beginPos2_4;
GArrowStringArrayBuilder *builder_sam2_4;

////////////////////////////////////////////////
GArrowArray *array_beginPos3;
GArrowArray *array_sam3;
GArrowInt32ArrayBuilder *builder_beginPos3;
GArrowStringArrayBuilder *builder_sam3;

GArrowArray *array_beginPos3_1;
GArrowArray *array_sam3_1;
GArrowInt32ArrayBuilder *builder_beginPos3_1;
GArrowStringArrayBuilder *builder_sam3_1;

GArrowArray *array_beginPos3_2;
GArrowArray *array_sam3_2;
GArrowInt32ArrayBuilder *builder_beginPos3_2;
GArrowStringArrayBuilder *builder_sam3_2;

GArrowArray *array_beginPos3_3;
GArrowArray *array_sam3_3;
GArrowInt32ArrayBuilder *builder_beginPos3_3;
GArrowStringArrayBuilder *builder_sam3_3;

////////////////////////////////////////////////
GArrowArray *array_beginPos4;
GArrowArray *array_sam4;
GArrowInt32ArrayBuilder *builder_beginPos4;
GArrowStringArrayBuilder *builder_sam4;

GArrowArray *array_beginPos4_1;
GArrowArray *array_sam4_1;
GArrowInt32ArrayBuilder *builder_beginPos4_1;
GArrowStringArrayBuilder *builder_sam4_1;

GArrowArray *array_beginPos4_2;
GArrowArray *array_sam4_2;
GArrowInt32ArrayBuilder *builder_beginPos4_2;
GArrowStringArrayBuilder *builder_sam4_2;

GArrowArray *array_beginPos4_3;
GArrowArray *array_sam4_3;
GArrowInt32ArrayBuilder *builder_beginPos4_3;
GArrowStringArrayBuilder *builder_sam4_3;

////////////////////////////////////////////////
GArrowArray *array_beginPos5;
GArrowArray *array_sam5;
GArrowInt32ArrayBuilder *builder_beginPos5;
GArrowStringArrayBuilder *builder_sam5;

GArrowArray *array_beginPos5_1;
GArrowArray *array_sam5_1;
GArrowInt32ArrayBuilder *builder_beginPos5_1;
GArrowStringArrayBuilder *builder_sam5_1;

GArrowArray *array_beginPos5_2;
GArrowArray *array_sam5_2;
GArrowInt32ArrayBuilder *builder_beginPos5_2;
GArrowStringArrayBuilder *builder_sam5_2;

GArrowArray *array_beginPos5_3;
GArrowArray *array_sam5_3;
GArrowInt32ArrayBuilder *builder_beginPos5_3;
GArrowStringArrayBuilder *builder_sam5_3;

////////////////////////////////////////////////
GArrowArray *array_beginPos6;
GArrowArray *array_sam6;
GArrowInt32ArrayBuilder *builder_beginPos6;
GArrowStringArrayBuilder *builder_sam6;

GArrowArray *array_beginPos6_1;
GArrowArray *array_sam6_1;
GArrowInt32ArrayBuilder *builder_beginPos6_1;
GArrowStringArrayBuilder *builder_sam6_1;

GArrowArray *array_beginPos6_2;
GArrowArray *array_sam6_2;
GArrowInt32ArrayBuilder *builder_beginPos6_2;
GArrowStringArrayBuilder *builder_sam6_2;

GArrowArray *array_beginPos6_3;
GArrowArray *array_sam6_3;
GArrowInt32ArrayBuilder *builder_beginPos6_3;
GArrowStringArrayBuilder *builder_sam6_3;

////////////////////////////////////////////////
GArrowArray *array_beginPos7;
GArrowArray *array_sam7;
GArrowInt32ArrayBuilder *builder_beginPos7;
GArrowStringArrayBuilder *builder_sam7;

GArrowArray *array_beginPos7_1;
GArrowArray *array_sam7_1;
GArrowInt32ArrayBuilder *builder_beginPos7_1;
GArrowStringArrayBuilder *builder_sam7_1;

GArrowArray *array_beginPos7_2;
GArrowArray *array_sam7_2;
GArrowInt32ArrayBuilder *builder_beginPos7_2;
GArrowStringArrayBuilder *builder_sam7_2;


////////////////////////////////////////////////
GArrowArray *array_beginPos8;
GArrowArray *array_sam8;
GArrowInt32ArrayBuilder *builder_beginPos8;
GArrowStringArrayBuilder *builder_sam8;

GArrowArray *array_beginPos8_1;
GArrowArray *array_sam8_1;
GArrowInt32ArrayBuilder *builder_beginPos8_1;
GArrowStringArrayBuilder *builder_sam8_1;

GArrowArray *array_beginPos8_2;
GArrowArray *array_sam8_2;
GArrowInt32ArrayBuilder *builder_beginPos8_2;
GArrowStringArrayBuilder *builder_sam8_2;

////////////////////////////////////////////////
GArrowArray *array_beginPos9;
GArrowArray *array_sam9;
GArrowInt32ArrayBuilder *builder_beginPos9;
GArrowStringArrayBuilder *builder_sam9;

GArrowArray *array_beginPos9_1;
GArrowArray *array_sam9_1;
GArrowInt32ArrayBuilder *builder_beginPos9_1;
GArrowStringArrayBuilder *builder_sam9_1;

GArrowArray *array_beginPos9_2;
GArrowArray *array_sam9_2;
GArrowInt32ArrayBuilder *builder_beginPos9_2;
GArrowStringArrayBuilder *builder_sam9_2;

////////////////////////////////////////////////
GArrowArray *array_beginPos10;
GArrowArray *array_sam10;
GArrowInt32ArrayBuilder *builder_beginPos10;
GArrowStringArrayBuilder *builder_sam10;

GArrowArray *array_beginPos10_1;
GArrowArray *array_sam10_1;
GArrowInt32ArrayBuilder *builder_beginPos10_1;
GArrowStringArrayBuilder *builder_sam10_1;

GArrowArray *array_beginPos10_2;
GArrowArray *array_sam10_2;
GArrowInt32ArrayBuilder *builder_beginPos10_2;
GArrowStringArrayBuilder *builder_sam10_2;

////////////////////////////////////////////////
GArrowArray *array_beginPos11;
GArrowArray *array_sam11;
GArrowInt32ArrayBuilder *builder_beginPos11;
GArrowStringArrayBuilder *builder_sam11;

GArrowArray *array_beginPos11_1;
GArrowArray *array_sam11_1;
GArrowInt32ArrayBuilder *builder_beginPos11_1;
GArrowStringArrayBuilder *builder_sam11_1;

GArrowArray *array_beginPos11_2;
GArrowArray *array_sam11_2;
GArrowInt32ArrayBuilder *builder_beginPos11_2;
GArrowStringArrayBuilder *builder_sam11_2;

////////////////////////////////////////////////
GArrowArray *array_beginPos12;
GArrowArray *array_sam12;
GArrowInt32ArrayBuilder *builder_beginPos12;
GArrowStringArrayBuilder *builder_sam12;

GArrowArray *array_beginPos12_1;
GArrowArray *array_sam12_1;
GArrowInt32ArrayBuilder *builder_beginPos12_1;
GArrowStringArrayBuilder *builder_sam12_1;

GArrowArray *array_beginPos12_2;
GArrowArray *array_sam12_2;
GArrowInt32ArrayBuilder *builder_beginPos12_2;
GArrowStringArrayBuilder *builder_sam12_2;

////////////////////////////////////////////////
GArrowArray *array_beginPos13;
GArrowArray *array_sam13;
GArrowInt32ArrayBuilder *builder_beginPos13;
GArrowStringArrayBuilder *builder_sam13;

GArrowArray *array_beginPos13_1;
GArrowArray *array_sam13_1;
GArrowInt32ArrayBuilder *builder_beginPos13_1;
GArrowStringArrayBuilder *builder_sam13_1;

////////////////////////////////////////////////
GArrowArray *array_beginPos14;
GArrowArray *array_sam14;
GArrowInt32ArrayBuilder *builder_beginPos14;
GArrowStringArrayBuilder *builder_sam14;

GArrowArray *array_beginPos14_1;
GArrowArray *array_sam14_1;
GArrowInt32ArrayBuilder *builder_beginPos14_1;
GArrowStringArrayBuilder *builder_sam14_1;

////////////////////////////////////////////////
GArrowArray *array_beginPos15;
GArrowArray *array_sam15;
GArrowInt32ArrayBuilder *builder_beginPos15;
GArrowStringArrayBuilder *builder_sam15;

GArrowArray *array_beginPos15_1;
GArrowArray *array_sam15_1;
GArrowInt32ArrayBuilder *builder_beginPos15_1;
GArrowStringArrayBuilder *builder_sam15_1;

////////////////////////////////////////////////
GArrowArray *array_beginPos16;
GArrowArray *array_sam16;
GArrowInt32ArrayBuilder *builder_beginPos16;
GArrowStringArrayBuilder *builder_sam16;

GArrowArray *array_beginPos16_1;
GArrowArray *array_sam16_1;
GArrowInt32ArrayBuilder *builder_beginPos16_1;
GArrowStringArrayBuilder *builder_sam16_1;

////////////////////////////////////////////////
GArrowArray *array_beginPos17;
GArrowArray *array_sam17;
GArrowInt32ArrayBuilder *builder_beginPos17;
GArrowStringArrayBuilder *builder_sam17;

GArrowArray *array_beginPos17_1;
GArrowArray *array_sam17_1;
GArrowInt32ArrayBuilder *builder_beginPos17_1;
GArrowStringArrayBuilder *builder_sam17_1;

////////////////////////////////////////////////
GArrowArray *array_beginPos18;
GArrowArray *array_sam18;
GArrowInt32ArrayBuilder *builder_beginPos18;
GArrowStringArrayBuilder *builder_sam18;

GArrowArray *array_beginPos18_1;
GArrowArray *array_sam18_1;
GArrowInt32ArrayBuilder *builder_beginPos18_1;
GArrowStringArrayBuilder *builder_sam18_1;


////////////////////////////////////////////////
GArrowArray *array_beginPos19;
GArrowArray *array_sam19;
GArrowInt32ArrayBuilder *builder_beginPos19;
GArrowStringArrayBuilder *builder_sam19;

////////////////////////////////////////////////
GArrowArray *array_beginPos20;
GArrowArray *array_sam20;
GArrowInt32ArrayBuilder *builder_beginPos20;
GArrowStringArrayBuilder *builder_sam20;

////////////////////////////////////////////////
GArrowArray *array_beginPos21;
GArrowArray *array_sam21;
GArrowInt32ArrayBuilder *builder_beginPos21;
GArrowStringArrayBuilder *builder_sam21;

////////////////////////////////////////////////
GArrowArray *array_beginPos22;
GArrowArray *array_sam22;
GArrowInt32ArrayBuilder *builder_beginPos22;
GArrowStringArrayBuilder *builder_sam22;


///////////////////////////////////////////////
GArrowArray *array_beginPosX;
GArrowArray *array_samX;
GArrowInt32ArrayBuilder *builder_beginPosX;
GArrowStringArrayBuilder *builder_samX;

GArrowArray *array_beginPosX_1;
GArrowArray *array_samX_1;
GArrowInt32ArrayBuilder *builder_beginPosX_1;
GArrowStringArrayBuilder *builder_samX_1;

GArrowArray *array_beginPosX_2;
GArrowArray *array_samX_2;
GArrowInt32ArrayBuilder *builder_beginPosX_2;
GArrowStringArrayBuilder *builder_samX_2;

////////////////////////////////////////////////
GArrowArray *array_beginPosY;
GArrowArray *array_samY;
GArrowInt32ArrayBuilder *builder_beginPosY;
GArrowStringArrayBuilder *builder_samY;

////////////////////////////////////////////////
GArrowArray *array_beginPosM;
GArrowArray *array_samM;
GArrowInt32ArrayBuilder *builder_beginPosM;
GArrowStringArrayBuilder *builder_samM;

////////////////////////////////////////////////


GArrowSchema* getSchema(void)
{
    GArrowSchema *schema;
    //RecordBatch creation
    GArrowField *f0 = garrow_field_new("beginPoss", GARROW_DATA_TYPE(garrow_int32_data_type_new()));
    GArrowField *f1 = garrow_field_new("sam", GARROW_DATA_TYPE(garrow_string_data_type_new()));

    GList *fields = NULL;
    fields = g_list_append(fields, f0);
    fields = g_list_append(fields, f1);

    //Create schema and free unnecessary fields
    schema = garrow_schema_new(fields);
    g_list_free(fields);
    g_object_unref(f0);
    g_object_unref(f1);

    return schema;
}

GArrowRecordBatch * create_arrow_record_batch(gint64 count, GArrowArray *array_beginPos,GArrowArray *array_sam)
{
    GArrowSchema *schema;
    GArrowRecordBatch *batch_genomics;

    schema = getSchema();

    GList *columns_genomics;
    columns_genomics = g_list_append(columns_genomics,array_beginPos);
    columns_genomics = g_list_append(columns_genomics,array_sam);

    batch_genomics = garrow_record_batch_new(schema,count,columns_genomics,NULL);

    g_list_free(columns_genomics);

    return batch_genomics;
}

void arrow_builders_start(void)
{
  builder_beginPos1 = garrow_int32_array_builder_new();
  builder_sam1 = garrow_string_array_builder_new();

  builder_beginPos1_1 = garrow_int32_array_builder_new();
  builder_sam1_1 = garrow_string_array_builder_new();

  builder_beginPos1_2 = garrow_int32_array_builder_new();
  builder_sam1_2 = garrow_string_array_builder_new();

  builder_beginPos1_3 = garrow_int32_array_builder_new();
  builder_sam1_3 = garrow_string_array_builder_new();

  builder_beginPos1_4 = garrow_int32_array_builder_new();
  builder_sam1_4 = garrow_string_array_builder_new();
///////////////////////////////////////////////////

  builder_beginPos2 = garrow_int32_array_builder_new();
  builder_sam2 = garrow_string_array_builder_new();

  builder_beginPos2_1 = garrow_int32_array_builder_new();
  builder_sam2_1 = garrow_string_array_builder_new();

  builder_beginPos2_2 = garrow_int32_array_builder_new();
  builder_sam2_2 = garrow_string_array_builder_new();

  builder_beginPos2_3 = garrow_int32_array_builder_new();
  builder_sam2_3 = garrow_string_array_builder_new();

  builder_beginPos2_4 = garrow_int32_array_builder_new();
  builder_sam2_4 = garrow_string_array_builder_new();
///////////////////////////////////////////////////

  builder_beginPos3 = garrow_int32_array_builder_new();
  builder_sam3 = garrow_string_array_builder_new();

  builder_beginPos3_1 = garrow_int32_array_builder_new();
  builder_sam3_1 = garrow_string_array_builder_new();

  builder_beginPos3_2 = garrow_int32_array_builder_new();
  builder_sam3_2 = garrow_string_array_builder_new();

  builder_beginPos3_3 = garrow_int32_array_builder_new();
  builder_sam3_3 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos4 = garrow_int32_array_builder_new();
  builder_sam4 = garrow_string_array_builder_new();

  builder_beginPos4_1 = garrow_int32_array_builder_new();
  builder_sam4_1 = garrow_string_array_builder_new();

  builder_beginPos4_2 = garrow_int32_array_builder_new();
  builder_sam4_2 = garrow_string_array_builder_new();

  builder_beginPos4_3 = garrow_int32_array_builder_new();
  builder_sam4_3 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos5 = garrow_int32_array_builder_new();
  builder_sam5 = garrow_string_array_builder_new();

  builder_beginPos5_1 = garrow_int32_array_builder_new();
  builder_sam5_1 = garrow_string_array_builder_new();

  builder_beginPos5_2 = garrow_int32_array_builder_new();
  builder_sam5_2 = garrow_string_array_builder_new();

  builder_beginPos5_3 = garrow_int32_array_builder_new();
  builder_sam5_3 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos6 = garrow_int32_array_builder_new();
  builder_sam6 = garrow_string_array_builder_new();

  builder_beginPos6_1 = garrow_int32_array_builder_new();
  builder_sam6_1 = garrow_string_array_builder_new();

  builder_beginPos6_2 = garrow_int32_array_builder_new();
  builder_sam6_2 = garrow_string_array_builder_new();

  builder_beginPos6_3 = garrow_int32_array_builder_new();
  builder_sam6_3 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos7 = garrow_int32_array_builder_new();
  builder_sam7 = garrow_string_array_builder_new();

  builder_beginPos7_1 = garrow_int32_array_builder_new();
  builder_sam7_1 = garrow_string_array_builder_new();

  builder_beginPos7_2 = garrow_int32_array_builder_new();
  builder_sam7_2 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos8 = garrow_int32_array_builder_new();
  builder_sam8 = garrow_string_array_builder_new();

  builder_beginPos8_1 = garrow_int32_array_builder_new();
  builder_sam8_1 = garrow_string_array_builder_new();

  builder_beginPos8_2 = garrow_int32_array_builder_new();
  builder_sam8_2 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos9 = garrow_int32_array_builder_new();
  builder_sam9 = garrow_string_array_builder_new();

  builder_beginPos9_1 = garrow_int32_array_builder_new();
  builder_sam9_1 = garrow_string_array_builder_new();

  builder_beginPos9_2 = garrow_int32_array_builder_new();
  builder_sam9_2 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos10 = garrow_int32_array_builder_new();
  builder_sam10 = garrow_string_array_builder_new();

  builder_beginPos10_1 = garrow_int32_array_builder_new();
  builder_sam10_1 = garrow_string_array_builder_new();

  builder_beginPos10_2 = garrow_int32_array_builder_new();
  builder_sam10_2 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos11 = garrow_int32_array_builder_new();
  builder_sam11 = garrow_string_array_builder_new();

  builder_beginPos11_1 = garrow_int32_array_builder_new();
  builder_sam11_1 = garrow_string_array_builder_new();

  builder_beginPos11_2 = garrow_int32_array_builder_new();
  builder_sam11_2 = garrow_string_array_builder_new();


///////////////////////////////////////////////////

  builder_beginPos12 = garrow_int32_array_builder_new();
  builder_sam12 = garrow_string_array_builder_new();

  builder_beginPos12_1 = garrow_int32_array_builder_new();
  builder_sam12_1 = garrow_string_array_builder_new();

  builder_beginPos12_2 = garrow_int32_array_builder_new();
  builder_sam12_2 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos13 = garrow_int32_array_builder_new();
  builder_sam13 = garrow_string_array_builder_new();

  builder_beginPos13_1 = garrow_int32_array_builder_new();
  builder_sam13_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos14 = garrow_int32_array_builder_new();
  builder_sam14 = garrow_string_array_builder_new();

  builder_beginPos14_1 = garrow_int32_array_builder_new();
  builder_sam14_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos15 = garrow_int32_array_builder_new();
  builder_sam15 = garrow_string_array_builder_new();

  builder_beginPos15_1 = garrow_int32_array_builder_new();
  builder_sam15_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos16 = garrow_int32_array_builder_new();
  builder_sam16 = garrow_string_array_builder_new();

  builder_beginPos16_1 = garrow_int32_array_builder_new();
  builder_sam16_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos17 = garrow_int32_array_builder_new();
  builder_sam17 = garrow_string_array_builder_new();

  builder_beginPos17_1 = garrow_int32_array_builder_new();
  builder_sam17_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos18 = garrow_int32_array_builder_new();
  builder_sam18 = garrow_string_array_builder_new();

  builder_beginPos18_1 = garrow_int32_array_builder_new();
  builder_sam18_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos19 = garrow_int32_array_builder_new();
  builder_sam19 = garrow_string_array_builder_new();
///////////////////////////////////////////////////

  builder_beginPos20 = garrow_int32_array_builder_new();
  builder_sam20 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos21 = garrow_int32_array_builder_new();
  builder_sam21 = garrow_string_array_builder_new();
///////////////////////////////////////////////////

  builder_beginPos22 = garrow_int32_array_builder_new();
  builder_sam22 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPosX = garrow_int32_array_builder_new();
  builder_samX = garrow_string_array_builder_new();

  builder_beginPosX_1 = garrow_int32_array_builder_new();
  builder_samX_1 = garrow_string_array_builder_new();

  builder_beginPosX_2 = garrow_int32_array_builder_new();
  builder_samX_2 = garrow_string_array_builder_new();


///////////////////////////////////////////////////

  builder_beginPosY = garrow_int32_array_builder_new();
  builder_samY = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPosM = garrow_int32_array_builder_new();
  builder_samM = garrow_string_array_builder_new();



}

gboolean
arrow_builders_append(gint32 builder_id, gint32 beginPos, const gchar *sam)
    {
        gboolean success = TRUE;
        GError *error = NULL;

        if(builder_id == 1) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam1, sam, &error);
            }
        }
	else if(builder_id == 110) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos1_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam1_1, sam, &error);
            }
        }
	else if(builder_id == 120) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos1_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam1_2, sam, &error);
            }
        }
        else if(builder_id == 130) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos1_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam1_3, sam, &error);
            }
        }
        else if(builder_id == 140) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos1_4, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam1_4, sam, &error);
            }
        }

else if(builder_id == 2) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam2, sam, &error);
            }
        }
	else if(builder_id == 210) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos2_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam2_1, sam, &error);
            }
        }
	else if(builder_id == 220) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos2_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam2_2, sam, &error);
            }
        }
        else if(builder_id == 230) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos2_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam2_3, sam, &error);
            }
        }
        else if(builder_id == 240) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos2_4, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam2_4, sam, &error);
            }
        }
else if(builder_id == 3) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam3, sam, &error);
            }
        }
	else if(builder_id == 31) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos3_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam3_1, sam, &error);
            }
        }
	else if(builder_id == 32) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos3_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam3_2, sam, &error);
            }
        }
        else if(builder_id == 33) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos3_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam3_3, sam, &error);
            }
        }

else if(builder_id == 4) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos4, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam4, sam, &error);
            }
        }
	else if(builder_id == 41) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos4_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam4_1, sam, &error);
            }
        }
	else if(builder_id == 42) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos4_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam4_2, sam, &error);
            }
        }
        else if(builder_id == 43) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos4_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam4_3, sam, &error);
            }
        }
else if(builder_id == 5) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos5, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam5, sam, &error);
            }
        }
	else if(builder_id == 51) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos5_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam5_1, sam, &error);
            }
        }
	else if(builder_id == 52) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos5_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam5_2, sam, &error);
            }
        }
        else if(builder_id == 53) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos5_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam5_3, sam, &error);
            }
        }
else if(builder_id == 6) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos6, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam6, sam, &error);
            }
        }
	else if(builder_id == 61) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos6_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam6_1, sam, &error);
            }
        }
	else if(builder_id == 62) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos6_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam6_2, sam, &error);
            }
        }
        else if(builder_id == 63) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos6_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam6_3, sam, &error);
            }
        }

else if(builder_id == 7) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos7, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam7, sam, &error);
            }
        }
	else if(builder_id == 71) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos7_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam7_1, sam, &error);
            }
        }
	else if(builder_id == 72) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos7_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam7_2, sam, &error);
            }
        }
else if(builder_id == 8) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos8, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam8, sam, &error);
            }
        }
	else if(builder_id == 81) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos8_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam8_1, sam, &error);
            }
        }
	else if(builder_id == 82) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos8_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam8_2, sam, &error);
            }
        }
else if(builder_id == 9) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos9, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam9, sam, &error);
            }
        }
	else if(builder_id == 91) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos9_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam9_1, sam, &error);
            }
        }
	else if(builder_id == 92) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos9_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam9_2, sam, &error);
            }
        }
else if(builder_id == 10) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos10, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam10, sam, &error);
            }
        }
	else if(builder_id == 101) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos10_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam10_1, sam, &error);
            }
        }
	else if(builder_id == 102) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos10_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam10_2, sam, &error);
            }
        }

else if(builder_id == 11) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos11, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam11, sam, &error);
            }
        }
	else if(builder_id == 111) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos11_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam11_1, sam, &error);
            }
        }
	else if(builder_id == 112) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos11_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam11_2, sam, &error);
            }
        }

else if(builder_id == 12) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos12, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam12, sam, &error);
            }
        }
	else if(builder_id == 121) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos12_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam12_1, sam, &error);
            }
        }
	else if(builder_id == 122) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos12_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam12_2, sam, &error);
            }
        }

else if(builder_id == 13) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos13, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam13, sam, &error);
            }
        }
	else if(builder_id == 131) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos13_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam13_1, sam, &error);
            }
        }

else if(builder_id == 14) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos14, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam14, sam, &error);
            }
        }
	else if(builder_id == 141) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos14_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam14_1, sam, &error);
            }
        }

else if(builder_id == 15) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos15, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam15, sam, &error);
            }
        }
	else if(builder_id == 151) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos15_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam15_1, sam, &error);
            }
        }

else if(builder_id == 16) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos16, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam16, sam, &error);
            }
        }
	else if(builder_id == 161) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos16_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam16_1, sam, &error);
            }
        }

else if(builder_id == 17) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos17, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam17, sam, &error);
            }
        }
	else if(builder_id == 171) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos17_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam17_1, sam, &error);
            }
        }

else if(builder_id == 18) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos18, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam18, sam, &error);
            }
        }
	else if(builder_id == 181) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos18_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam18_1, sam, &error);
            }
        }

else if(builder_id == 19) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos19, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam19, sam, &error);
            }
        }
else if(builder_id == 20) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos20, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam20, sam, &error);
            }
        }
else if(builder_id == 21) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos21, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam21, sam, &error);
            }
        }
else if(builder_id == 22) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos22, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam22, sam, &error);
            }
        }

else if(builder_id == 23) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosX, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_samX, sam, &error);
            }
        }
	else if(builder_id == 231) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosX_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_samX_1, sam, &error);
            }
        }
	else if(builder_id == 232) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosX_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_samX_2, sam, &error);
            }
        }


else if(builder_id == 24) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosY, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_samY, sam, &error);
            }
        }
else if(builder_id == 25) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosM, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_samM, sam, &error);
            }
        }


   return success;
}

GArrowRecordBatch *
arrow_builders_finish(gint32 builder_id, gint64 count)
{
 GError *error = NULL;
 GArrowRecordBatch *batch_genomics;
    if(builder_id == 1) {
        array_beginPos1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos1), &error);
        array_sam1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam1), &error);
        g_object_unref(builder_beginPos1);
        g_object_unref(builder_sam1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos1,array_sam1);

        g_object_unref(array_beginPos1);
        g_object_unref(array_sam1);
    }
    else if(builder_id == 110) {
        array_beginPos1_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos1_1), &error);
	array_sam1_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam1_1), &error);
        g_object_unref(builder_beginPos1_1);
        g_object_unref(builder_sam1_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos1_1,array_sam1_1);

	g_object_unref(array_beginPos1_1);
        g_object_unref(array_sam1_1);   
    }
    else if(builder_id == 120) {
        array_beginPos1_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos1_2), &error);
        array_sam1_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam1_2), &error);
        g_object_unref(builder_beginPos1_2);
        g_object_unref(builder_sam1_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos1_2,array_sam1_2);

        g_object_unref(array_beginPos1_2);
        g_object_unref(array_sam1_2);
    }
    else if(builder_id == 130) {
        array_beginPos1_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos1_3), &error);
        array_sam1_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam1_3), &error);
        g_object_unref(builder_beginPos1_3);
        g_object_unref(builder_sam1_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos1_3,array_sam1_3);

        g_object_unref(array_beginPos1_3);
        g_object_unref(array_sam1_3);
    }
    else if(builder_id == 140) {
        array_beginPos1_4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos1_4), &error);
        array_sam1_4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam1_4), &error);
        g_object_unref(builder_beginPos1_4);
        g_object_unref(builder_sam1_4);

        batch_genomics = create_arrow_record_batch(count, array_beginPos1_4,array_sam1_4);

        g_object_unref(array_beginPos1_4);
        g_object_unref(array_sam1_4);
    }

if(builder_id == 2) {
        array_beginPos2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos2), &error);
        array_sam2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam2), &error);
        g_object_unref(builder_beginPos2);
        g_object_unref(builder_sam2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos2,array_sam2);

        g_object_unref(array_beginPos2);
        g_object_unref(array_sam2);
    }
    else if(builder_id == 210) {
        array_beginPos2_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos2_1), &error);
	array_sam2_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam2_1), &error);
        g_object_unref(builder_beginPos2_1);
        g_object_unref(builder_sam2_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos2_1,array_sam2_1);

	g_object_unref(array_beginPos2_1);
        g_object_unref(array_sam2_1);   
    }
    else if(builder_id == 220) {
        array_beginPos2_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos2_2), &error);
        array_sam2_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam2_2), &error);
        g_object_unref(builder_beginPos2_2);
        g_object_unref(builder_sam2_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos2_2,array_sam2_2);

        g_object_unref(array_beginPos2_2);
        g_object_unref(array_sam2_2);
    }
    else if(builder_id == 230) {
        array_beginPos2_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos2_3), &error);
        array_sam2_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam2_3), &error);
        g_object_unref(builder_beginPos2_3);
        g_object_unref(builder_sam2_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos2_3,array_sam2_3);

        g_object_unref(array_beginPos2_3);
        g_object_unref(array_sam2_3);
    }
    else if(builder_id == 240) {
        array_beginPos2_4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos2_4), &error);
        array_sam2_4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam2_4), &error);
        g_object_unref(builder_beginPos2_4);
        g_object_unref(builder_sam2_4);

        batch_genomics = create_arrow_record_batch(count, array_beginPos2_4,array_sam2_4);

        g_object_unref(array_beginPos2_4);
        g_object_unref(array_sam2_4);
    }

if(builder_id == 3) {
        array_beginPos3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos3), &error);
        array_sam3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam3), &error);
        g_object_unref(builder_beginPos3);
        g_object_unref(builder_sam3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos3,array_sam3);

        g_object_unref(array_beginPos3);
        g_object_unref(array_sam3);
    }
    else if(builder_id == 31) {
        array_beginPos3_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos3_1), &error);
	array_sam3_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam3_1), &error);
        g_object_unref(builder_beginPos3_1);
        g_object_unref(builder_sam3_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos3_1,array_sam3_1);

	g_object_unref(array_beginPos3_1);
        g_object_unref(array_sam3_1);   
    }
    else if(builder_id == 32) {
        array_beginPos3_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos3_2), &error);
        array_sam3_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam3_2), &error);
        g_object_unref(builder_beginPos3_2);
        g_object_unref(builder_sam3_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos3_2,array_sam3_2);

        g_object_unref(array_beginPos3_2);
        g_object_unref(array_sam3_2);
    }
    else if(builder_id == 33) {
        array_beginPos3_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos3_3), &error);
        array_sam3_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam3_3), &error);
        g_object_unref(builder_beginPos3_3);
        g_object_unref(builder_sam3_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos3_3,array_sam3_3);

        g_object_unref(array_beginPos3_3);
        g_object_unref(array_sam3_3);
    }


if(builder_id == 4) {
        array_beginPos4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos4), &error);
        array_sam4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam4), &error);
        g_object_unref(builder_beginPos4);
        g_object_unref(builder_sam4);

        batch_genomics = create_arrow_record_batch(count, array_beginPos4,array_sam4);

        g_object_unref(array_beginPos4);
        g_object_unref(array_sam4);
    }
    else if(builder_id == 41) {
        array_beginPos4_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos4_1), &error);
	array_sam4_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam4_1), &error);
        g_object_unref(builder_beginPos4_1);
        g_object_unref(builder_sam4_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos4_1,array_sam4_1);

	g_object_unref(array_beginPos4_1);
        g_object_unref(array_sam4_1);   
    }
    else if(builder_id == 42) {
        array_beginPos4_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos4_2), &error);
        array_sam4_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam4_2), &error);
        g_object_unref(builder_beginPos4_2);
        g_object_unref(builder_sam4_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos4_2,array_sam4_2);

        g_object_unref(array_beginPos4_2);
        g_object_unref(array_sam4_2);
    }
    else if(builder_id == 43) {
        array_beginPos4_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos4_3), &error);
        array_sam4_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam4_3), &error);
        g_object_unref(builder_beginPos4_3);
        g_object_unref(builder_sam4_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos4_3,array_sam4_3);

        g_object_unref(array_beginPos4_3);
        g_object_unref(array_sam4_3);
    }

if(builder_id == 5) {
        array_beginPos5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos5), &error);
        array_sam5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam5), &error);
        g_object_unref(builder_beginPos5);
        g_object_unref(builder_sam5);

        batch_genomics = create_arrow_record_batch(count, array_beginPos5,array_sam5);

        g_object_unref(array_beginPos5);
        g_object_unref(array_sam5);
    }
    else if(builder_id == 51) {
        array_beginPos5_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos5_1), &error);
	array_sam5_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam5_1), &error);
        g_object_unref(builder_beginPos5_1);
        g_object_unref(builder_sam5_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos5_1,array_sam5_1);

	g_object_unref(array_beginPos5_1);
        g_object_unref(array_sam5_1);   
    }
    else if(builder_id == 52) {
        array_beginPos5_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos5_2), &error);
        array_sam5_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam5_2), &error);
        g_object_unref(builder_beginPos5_2);
        g_object_unref(builder_sam5_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos5_2,array_sam5_2);

        g_object_unref(array_beginPos5_2);
        g_object_unref(array_sam5_2);
    }
    else if(builder_id == 53) {
        array_beginPos5_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos5_3), &error);
        array_sam5_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam5_3), &error);
        g_object_unref(builder_beginPos5_3);
        g_object_unref(builder_sam5_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos5_3,array_sam5_3);

        g_object_unref(array_beginPos5_3);
        g_object_unref(array_sam5_3);
    }

if(builder_id == 6) {
        array_beginPos6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos6), &error);
        array_sam6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam6), &error);
        g_object_unref(builder_beginPos6);
        g_object_unref(builder_sam6);

        batch_genomics = create_arrow_record_batch(count, array_beginPos6,array_sam6);

        g_object_unref(array_beginPos6);
        g_object_unref(array_sam6);
    }
    else if(builder_id == 61) {
        array_beginPos6_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos6_1), &error);
	array_sam6_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam6_1), &error);
        g_object_unref(builder_beginPos6_1);
        g_object_unref(builder_sam6_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos6_1,array_sam6_1);

	g_object_unref(array_beginPos6_1);
        g_object_unref(array_sam6_1);   
    }
    else if(builder_id == 62) {
        array_beginPos6_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos6_2), &error);
        array_sam6_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam6_2), &error);
        g_object_unref(builder_beginPos6_2);
        g_object_unref(builder_sam6_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos6_2,array_sam6_2);

        g_object_unref(array_beginPos6_2);
        g_object_unref(array_sam6_2);
    }
    else if(builder_id == 63) {
        array_beginPos6_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos6_3), &error);
        array_sam6_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam6_3), &error);
        g_object_unref(builder_beginPos6_3);
        g_object_unref(builder_sam6_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos6_3,array_sam6_3);

        g_object_unref(array_beginPos6_3);
        g_object_unref(array_sam6_3);
    }
if(builder_id == 7) {
        array_beginPos7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos7), &error);
        array_sam7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam7), &error);
        g_object_unref(builder_beginPos7);
        g_object_unref(builder_sam7);

        batch_genomics = create_arrow_record_batch(count, array_beginPos7,array_sam7);

        g_object_unref(array_beginPos7);
        g_object_unref(array_sam7);
    }
    else if(builder_id == 71) {
        array_beginPos7_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos7_1), &error);
	array_sam7_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam7_1), &error);
        g_object_unref(builder_beginPos7_1);
        g_object_unref(builder_sam7_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos7_1,array_sam7_1);

	g_object_unref(array_beginPos7_1);
        g_object_unref(array_sam7_1);   
    }
    else if(builder_id == 72) {
        array_beginPos7_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos7_2), &error);
        array_sam7_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam7_2), &error);
        g_object_unref(builder_beginPos7_2);
        g_object_unref(builder_sam7_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos7_2,array_sam7_2);

        g_object_unref(array_beginPos7_2);
        g_object_unref(array_sam7_2);
    }
if(builder_id == 8) {
        array_beginPos8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos8), &error);
        array_sam8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam8), &error);
        g_object_unref(builder_beginPos8);
        g_object_unref(builder_sam8);

        batch_genomics = create_arrow_record_batch(count, array_beginPos8,array_sam8);

        g_object_unref(array_beginPos8);
        g_object_unref(array_sam8);
    }
    else if(builder_id == 81) {
        array_beginPos8_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos8_1), &error);
	array_sam8_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam8_1), &error);
        g_object_unref(builder_beginPos8_1);
        g_object_unref(builder_sam8_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos8_1,array_sam8_1);

	g_object_unref(array_beginPos8_1);
        g_object_unref(array_sam8_1);   
    }
    else if(builder_id == 82) {
        array_beginPos8_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos8_2), &error);
        array_sam8_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam8_2), &error);
        g_object_unref(builder_beginPos8_2);
        g_object_unref(builder_sam8_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos8_2,array_sam8_2);

        g_object_unref(array_beginPos8_2);
        g_object_unref(array_sam8_2);
    }
if(builder_id == 9) {
        array_beginPos9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos9), &error);
        array_sam9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam9), &error);
        g_object_unref(builder_beginPos9);
        g_object_unref(builder_sam9);

        batch_genomics = create_arrow_record_batch(count, array_beginPos9,array_sam9);

        g_object_unref(array_beginPos9);
        g_object_unref(array_sam9);
    }
    else if(builder_id == 91) {
        array_beginPos9_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos9_1), &error);
	array_sam9_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam9_1), &error);
        g_object_unref(builder_beginPos9_1);
        g_object_unref(builder_sam9_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos9_1,array_sam9_1);

	g_object_unref(array_beginPos9_1);
        g_object_unref(array_sam9_1);   
    }
    else if(builder_id == 92) {
        array_beginPos9_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos9_2), &error);
        array_sam9_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam9_2), &error);
        g_object_unref(builder_beginPos9_2);
        g_object_unref(builder_sam9_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos9_2,array_sam9_2);

        g_object_unref(array_beginPos9_2);
        g_object_unref(array_sam9_2);
    }
if(builder_id == 10) {
        array_beginPos10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos10), &error);
        array_sam10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam10), &error);
        g_object_unref(builder_beginPos10);
        g_object_unref(builder_sam10);

        batch_genomics = create_arrow_record_batch(count, array_beginPos10,array_sam10);

        g_object_unref(array_beginPos10);
        g_object_unref(array_sam10);
    }
    else if(builder_id == 101) {
        array_beginPos10_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos10_1), &error);
	array_sam10_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam10_1), &error);
        g_object_unref(builder_beginPos10_1);
        g_object_unref(builder_sam10_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos10_1,array_sam10_1);

	g_object_unref(array_beginPos10_1);
        g_object_unref(array_sam10_1);   
    }
    else if(builder_id == 102) {
        array_beginPos10_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos10_2), &error);
        array_sam10_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam10_2), &error);
        g_object_unref(builder_beginPos10_2);
        g_object_unref(builder_sam10_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos10_2,array_sam10_2);

        g_object_unref(array_beginPos10_2);
        g_object_unref(array_sam10_2);
    }
if(builder_id == 11) {
        array_beginPos11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos11), &error);
        array_sam11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam11), &error);
        g_object_unref(builder_beginPos11);
        g_object_unref(builder_sam11);

        batch_genomics = create_arrow_record_batch(count, array_beginPos11,array_sam11);

        g_object_unref(array_beginPos11);
        g_object_unref(array_sam11);
    }
    else if(builder_id == 111) {
        array_beginPos11_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos11_1), &error);
	array_sam11_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam11_1), &error);
        g_object_unref(builder_beginPos11_1);
        g_object_unref(builder_sam11_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos11_1,array_sam11_1);

	g_object_unref(array_beginPos11_1);
        g_object_unref(array_sam11_1);   
    }
    else if(builder_id == 112) {
        array_beginPos11_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos11_2), &error);
        array_sam11_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam11_2), &error);
        g_object_unref(builder_beginPos11_2);
        g_object_unref(builder_sam11_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos11_2,array_sam11_2);

        g_object_unref(array_beginPos11_2);
        g_object_unref(array_sam11_2);
    }
if(builder_id == 12) {
        array_beginPos12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos12), &error);
        array_sam12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam12), &error);
        g_object_unref(builder_beginPos12);
        g_object_unref(builder_sam12);

        batch_genomics = create_arrow_record_batch(count, array_beginPos12,array_sam12);

        g_object_unref(array_beginPos12);
        g_object_unref(array_sam12);
    }
    else if(builder_id == 121) {
        array_beginPos12_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos12_1), &error);
	array_sam12_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam12_1), &error);
        g_object_unref(builder_beginPos12_1);
        g_object_unref(builder_sam12_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos12_1,array_sam12_1);

	g_object_unref(array_beginPos12_1);
        g_object_unref(array_sam12_1);   
    }
    else if(builder_id == 122) {
        array_beginPos12_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos12_2), &error);
        array_sam12_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam12_2), &error);
        g_object_unref(builder_beginPos12_2);
        g_object_unref(builder_sam12_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos12_2,array_sam12_2);

        g_object_unref(array_beginPos12_2);
        g_object_unref(array_sam12_2);
    }
if(builder_id == 13) {
        array_beginPos13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos13), &error);
        array_sam13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam13), &error);
        g_object_unref(builder_beginPos13);
        g_object_unref(builder_sam13);

        batch_genomics = create_arrow_record_batch(count, array_beginPos13,array_sam13);

        g_object_unref(array_beginPos13);
        g_object_unref(array_sam13);
    }
    else if(builder_id == 131) {
        array_beginPos13_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos13_1), &error);
	array_sam13_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam13_1), &error);
        g_object_unref(builder_beginPos13_1);
        g_object_unref(builder_sam13_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos13_1,array_sam13_1);

	g_object_unref(array_beginPos13_1);
        g_object_unref(array_sam13_1);   
    }

if(builder_id == 14) {
        array_beginPos14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos14), &error);
        array_sam14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam14), &error);
        g_object_unref(builder_beginPos14);
        g_object_unref(builder_sam14);

        batch_genomics = create_arrow_record_batch(count, array_beginPos14,array_sam14);

        g_object_unref(array_beginPos14);
        g_object_unref(array_sam14);
    }
    else if(builder_id == 141) {
        array_beginPos14_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos14_1), &error);
	array_sam14_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam14_1), &error);
        g_object_unref(builder_beginPos14_1);
        g_object_unref(builder_sam14_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos14_1,array_sam14_1);

	g_object_unref(array_beginPos14_1);
        g_object_unref(array_sam14_1);   
    }

if(builder_id == 15) {
        array_beginPos15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos15), &error);
        array_sam15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam15), &error);
        g_object_unref(builder_beginPos15);
        g_object_unref(builder_sam15);

        batch_genomics = create_arrow_record_batch(count, array_beginPos15,array_sam15);

        g_object_unref(array_beginPos15);
        g_object_unref(array_sam15);
    }
    else if(builder_id == 151) {
        array_beginPos15_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos15_1), &error);
	array_sam15_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam15_1), &error);
        g_object_unref(builder_beginPos15_1);
        g_object_unref(builder_sam15_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos15_1,array_sam15_1);

	g_object_unref(array_beginPos15_1);
        g_object_unref(array_sam15_1);   
    }

if(builder_id == 16) {
        array_beginPos16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos16), &error);
        array_sam16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam16), &error);
        g_object_unref(builder_beginPos16);
        g_object_unref(builder_sam16);

        batch_genomics = create_arrow_record_batch(count, array_beginPos16,array_sam16);

        g_object_unref(array_beginPos16);
        g_object_unref(array_sam16);
    }
    else if(builder_id == 161) {
        array_beginPos16_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos16_1), &error);
	array_sam16_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam16_1), &error);
        g_object_unref(builder_beginPos16_1);
        g_object_unref(builder_sam16_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos16_1,array_sam16_1);

	g_object_unref(array_beginPos16_1);
        g_object_unref(array_sam16_1);   
    }
if(builder_id == 17) {
        array_beginPos17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos17), &error);
        array_sam17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam17), &error);
        g_object_unref(builder_beginPos17);
        g_object_unref(builder_sam17);

        batch_genomics = create_arrow_record_batch(count, array_beginPos17,array_sam17);

        g_object_unref(array_beginPos17);
        g_object_unref(array_sam17);
    }
    else if(builder_id == 171) {
        array_beginPos17_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos17_1), &error);
	array_sam17_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam17_1), &error);
        g_object_unref(builder_beginPos17_1);
        g_object_unref(builder_sam17_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos17_1,array_sam17_1);

	g_object_unref(array_beginPos17_1);
        g_object_unref(array_sam17_1);   
    }
if(builder_id == 18) {
        array_beginPos18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos18), &error);
        array_sam18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam18), &error);
        g_object_unref(builder_beginPos18);
        g_object_unref(builder_sam18);

        batch_genomics = create_arrow_record_batch(count, array_beginPos18,array_sam18);

        g_object_unref(array_beginPos18);
        g_object_unref(array_sam18);
    }
    else if(builder_id == 181) {
        array_beginPos18_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos18_1), &error);
	array_sam18_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam18_1), &error);
        g_object_unref(builder_beginPos18_1);
        g_object_unref(builder_sam18_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos18_1,array_sam18_1);

	g_object_unref(array_beginPos18_1);
        g_object_unref(array_sam18_1);   
    }

if(builder_id == 19) {
        array_beginPos19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos19), &error);
        array_sam19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam19), &error);
        g_object_unref(builder_beginPos19);
        g_object_unref(builder_sam19);

        batch_genomics = create_arrow_record_batch(count, array_beginPos19,array_sam19);

        g_object_unref(array_beginPos19);
        g_object_unref(array_sam19);
    }

if(builder_id == 20) {
        array_beginPos20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos20), &error);
        array_sam20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam20), &error);
        g_object_unref(builder_beginPos20);
        g_object_unref(builder_sam20);

        batch_genomics = create_arrow_record_batch(count, array_beginPos20,array_sam20);

        g_object_unref(array_beginPos20);
        g_object_unref(array_sam20);
    }
if(builder_id == 21) {
        array_beginPos21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos21), &error);
        array_sam21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam21), &error);
        g_object_unref(builder_beginPos21);
        g_object_unref(builder_sam21);

        batch_genomics = create_arrow_record_batch(count, array_beginPos21,array_sam21);

        g_object_unref(array_beginPos21);
        g_object_unref(array_sam21);
    }
if(builder_id == 22) {
        array_beginPos22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos22), &error);
        array_sam22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam22), &error);
        g_object_unref(builder_beginPos22);
        g_object_unref(builder_sam22);

        batch_genomics = create_arrow_record_batch(count, array_beginPos22,array_sam22);

        g_object_unref(array_beginPos22);
        g_object_unref(array_sam22);
    }

if(builder_id == 23) {
        array_beginPosX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosX), &error);
        array_samX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_samX), &error);
        g_object_unref(builder_beginPosX);
        g_object_unref(builder_samX);

        batch_genomics = create_arrow_record_batch(count, array_beginPosX,array_samX);

        g_object_unref(array_beginPosX);
        g_object_unref(array_samX);
    }
    else if(builder_id == 231) {
        array_beginPosX_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosX_1), &error);
	array_samX_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_samX_1), &error);
        g_object_unref(builder_beginPosX_1);
        g_object_unref(builder_samX_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPosX_1,array_samX_1);

	g_object_unref(array_beginPosX_1);
        g_object_unref(array_samX_1);   
    }
    else if(builder_id == 232) {
        array_beginPosX_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosX_2), &error);
        array_samX_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_samX_2), &error);
        g_object_unref(builder_beginPosX_2);
        g_object_unref(builder_samX_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPosX_2,array_samX_2);

        g_object_unref(array_beginPosX_2);
        g_object_unref(array_samX_2);
    }

if(builder_id == 24) {
        array_beginPosY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosY), &error);
        array_samY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_samY), &error);
        g_object_unref(builder_beginPosY);
        g_object_unref(builder_samY);

        batch_genomics = create_arrow_record_batch(count, array_beginPosY,array_samY);

        g_object_unref(array_beginPosY);
        g_object_unref(array_samY);
    }

if(builder_id == 25) {
        array_beginPosM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosM), &error);
        array_samM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_samM), &error);
        g_object_unref(builder_beginPosM);
        g_object_unref(builder_samM);

        batch_genomics = create_arrow_record_batch(count, array_beginPosM,array_samM);

        g_object_unref(array_beginPosM);
        g_object_unref(array_samM);
    }

    return batch_genomics;
}
/////////////////////////////////////////////////////
//		Arrow-Functionality                //
/////////////////////////////////////////////////////
char* generateRandomString(int size) {
    int i;
    int x;
    char *res = malloc(size + 1);
    //srand((unsigned int)time(NULL));
    for(i = 0; i < size; i++) {
        res[i] = (char) (rand()%(ASCII_END-ASCII_START))+ASCII_START;
    }
    res[size + 1] = '\0';
    return res;
}

void create_plasma_object(GArrowRecordBatch *batch_genomics)
{
    const char* id_arr[20];
    char objID_file[] = "/dev/shm/objID.txt";
    memcpy(id_arr, generateRandomString(20),20);

    FILE *fOut;
    fOut = fopen(objID_file, "a");
    fputs(id_arr, fOut);
    //fputs('\t', fOut);
    //fputs(host, fOut);
    fputc('\n', fOut);
    fclose(fOut);

    gboolean success = TRUE;
    GError *error = NULL;

    GPlasmaClient *gPlasmaClient;
    GPlasmaObjectID *object_id;
    GPlasmaClientCreateOptions *create_options;
    GPlasmaClientOptions *gplasmaClient_options;
    GArrowWriteOptions *garrowwriter_options;
    GPlasmaCreatedObject *Object;
    GArrowBuffer *arrowBuffer;

    //err_fputs(id_arr, stdout);
    //err_fputs("\n", stdout);

    //arrowBuffer = GSerializeRecordBatch(batch_genomics);
    garrowwriter_options = garrow_write_options_new();
    arrowBuffer = garrow_record_batch_serialize(batch_genomics, garrowwriter_options, &error);
    gint64 size = garrow_buffer_get_size(arrowBuffer);
    GBytes *bytes = garrow_buffer_get_data(arrowBuffer);
    gconstpointer *rbdata = g_bytes_get_data(bytes, &size);
    g_bytes_unref(bytes);

    //g_print("obj_id: %s\n", id_arr);
    fprintf(stderr, "[%s] obj_id: %s , size: %ld \n", __func__, id_arr, size);

    create_options = gplasma_client_create_options_new();
    gplasmaClient_options = gplasma_client_options_new();
    gPlasmaClient = gplasma_client_new("/tmp/plasma",gplasmaClient_options, &error);
    object_id = gplasma_object_id_new(id_arr, 20, &error);

    {
        // It should be guint8 instead of gchar. We use gchar here just for convenient. 
        guint8 metadata[] = "metadata";
        gplasma_client_create_options_set_metadata(create_options, (const guint8 *)metadata, sizeof(metadata));
    }
    Object = gplasma_client_create(gPlasmaClient, object_id, size, create_options, &error);

    g_object_unref(create_options);
    {
        GArrowBuffer *data;
        g_object_get(Object, "data", &data, NULL);
        //garrow_mutable_buffer_set_data(GARROW_MUTABLE_BUFFER(data),0,arrowBuffer,&error);
        garrow_mutable_buffer_set_data(GARROW_MUTABLE_BUFFER(data),0, rbdata,size,&error);
        g_object_unref(data);
    }

    gplasma_created_object_seal(Object, &error);
    g_object_unref(Object);
    gplasma_client_disconnect(gPlasmaClient, &error);
    g_object_unref(gPlasmaClient);
    g_object_unref(object_id);
}

void arrow_plasma_create(int id, long size) {
    GArrowRecordBatch * rb_genomics;
    rb_genomics=arrow_builders_finish(id, size);
    //fprintf(stderr, "[%s] id:%d, counts: %ld \n", __func__, id, size);
    //{
    //    g_print("%s", garrow_record_batch_to_string(rb_genomics, NULL));
    //    fprintf(stderr, "[%s] Arrow: %s\n", __func__, garrow_record_batch_to_string(rb_genomics, NULL));
    //}
    create_plasma_object(rb_genomics);
    g_object_unref(rb_genomics);
}
int exist(const char *name)
{
    struct stat   buffer;
    return (stat (name, &buffer) == 0);
}

void mm_arrow_start(void){
    char file[] = "/dev/shm/objID.txt";
    if(exist(file))
      remove(file);
    arrow_builders_start();
}

void mm_arrow_finish(void){
    arrow_plasma_create(1,counts1); 
    arrow_plasma_create(110,counts1_1);
    arrow_plasma_create(120,counts1_2);
    arrow_plasma_create(130,counts1_3);
    arrow_plasma_create(140,counts1_4);

    arrow_plasma_create(2,counts2); 
    arrow_plasma_create(210,counts2_1);
    arrow_plasma_create(220,counts2_2);
    arrow_plasma_create(230,counts2_3);
    arrow_plasma_create(240,counts2_4);

    arrow_plasma_create(3,counts3); 
    arrow_plasma_create(31,counts3_1);
    arrow_plasma_create(32,counts3_2);
    arrow_plasma_create(33,counts3_3);

    arrow_plasma_create(4,counts4); 
    arrow_plasma_create(41,counts4_1);
    arrow_plasma_create(42,counts4_2);
    arrow_plasma_create(43,counts4_3);

    arrow_plasma_create(5,counts5); 
    arrow_plasma_create(51,counts5_1);
    arrow_plasma_create(52,counts5_2);
    arrow_plasma_create(53,counts5_3);
    arrow_plasma_create(6,counts6); 
    arrow_plasma_create(61,counts6_1);
    arrow_plasma_create(62,counts6_2);
    arrow_plasma_create(63,counts6_3);

    arrow_plasma_create(7,counts7); 
    arrow_plasma_create(71,counts7_1);
    arrow_plasma_create(72,counts7_2);

    arrow_plasma_create(8,counts8); 
    arrow_plasma_create(81,counts8_1);
    arrow_plasma_create(82,counts8_2);

    arrow_plasma_create(9,counts9); 
    arrow_plasma_create(91,counts9_1);
    arrow_plasma_create(92,counts9_2);

    arrow_plasma_create(10,counts10); 
    arrow_plasma_create(101,counts10_1);
    arrow_plasma_create(102,counts10_2);

    arrow_plasma_create(11,counts11); 
    arrow_plasma_create(111,counts11_1);
    arrow_plasma_create(112,counts11_2);

    arrow_plasma_create(12,counts12); 
    arrow_plasma_create(121,counts12_1);
    arrow_plasma_create(122,counts12_2);

    arrow_plasma_create(13,counts13); 
    arrow_plasma_create(131,counts13_1);

    arrow_plasma_create(14,counts14); 
    arrow_plasma_create(141,counts14_1);

    arrow_plasma_create(15,counts15); 
    arrow_plasma_create(151,counts15_1);

    arrow_plasma_create(16,counts16); 
    arrow_plasma_create(161,counts16_1);

    arrow_plasma_create(17,counts17); 
    arrow_plasma_create(171,counts17_1);

    arrow_plasma_create(18,counts18); 
    arrow_plasma_create(181,counts18_1);

    arrow_plasma_create(19,counts19);
    arrow_plasma_create(20,counts20);
    arrow_plasma_create(21,counts21);
    arrow_plasma_create(22,counts22);

    arrow_plasma_create(23,countsX);
    arrow_plasma_create(231,countsX_1);
    arrow_plasma_create(232,countsX_2);

    arrow_plasma_create(24,countsY);
    arrow_plasma_create(25,countsM);
}

////////////////////////END////////////////////////////


struct mm_tbuf_s {
	void *km;
	int rep_len, frag_gap;
};

mm_tbuf_t *mm_tbuf_init(void)
{
	mm_tbuf_t *b;
	b = (mm_tbuf_t*)calloc(1, sizeof(mm_tbuf_t));
	if (!(mm_dbg_flag & 1)) b->km = km_init();
	return b;
}

void mm_tbuf_destroy(mm_tbuf_t *b)
{
	if (b == 0) return;
	km_destroy(b->km);
	free(b);
}

void *mm_tbuf_get_km(mm_tbuf_t *b)
{
	return b->km;
}

static int mm_dust_minier(void *km, int n, mm128_t *a, int l_seq, const char *seq, int sdust_thres)
{
	int n_dreg, j, k, u = 0;
	const uint64_t *dreg;
	sdust_buf_t *sdb;
	if (sdust_thres <= 0) return n;
	sdb = sdust_buf_init(km);
	dreg = sdust_core((const uint8_t*)seq, l_seq, sdust_thres, 64, &n_dreg, sdb);
	for (j = k = 0; j < n; ++j) { // squeeze out minimizers that significantly overlap with LCRs
		int32_t qpos = (uint32_t)a[j].y>>1, span = a[j].x&0xff;
		int32_t s = qpos - (span - 1), e = s + span;
		while (u < n_dreg && (int32_t)dreg[u] <= s) ++u;
		if (u < n_dreg && (int32_t)(dreg[u]>>32) < e) {
			int v, l = 0;
			for (v = u; v < n_dreg && (int32_t)(dreg[v]>>32) < e; ++v) { // iterate over LCRs overlapping this minimizer
				int ss = s > (int32_t)(dreg[v]>>32)? s : dreg[v]>>32;
				int ee = e < (int32_t)dreg[v]? e : (uint32_t)dreg[v];
				l += ee - ss;
			}
			if (l <= span>>1) a[k++] = a[j]; // keep the minimizer if less than half of it falls in masked region
		} else a[k++] = a[j];
	}
	sdust_buf_destroy(sdb);
	return k; // the new size
}

static void collect_minimizers(void *km, const mm_mapopt_t *opt, const mm_idx_t *mi, int n_segs, const int *qlens, const char **seqs, mm128_v *mv)
{
	int i, n, sum = 0;
	mv->n = 0;
	for (i = n = 0; i < n_segs; ++i) {
		size_t j;
		mm_sketch(km, seqs[i], qlens[i], mi->w, mi->k, i, mi->flag&MM_I_HPC, mv);
		for (j = n; j < mv->n; ++j)
			mv->a[j].y += sum << 1;
		if (opt->sdust_thres > 0) // mask low-complexity minimizers
			mv->n = n + mm_dust_minier(km, mv->n - n, mv->a + n, qlens[i], seqs[i], opt->sdust_thres);
		sum += qlens[i], n = mv->n;
	}
}

#include "ksort.h"
#define heap_lt(a, b) ((a).x > (b).x)
KSORT_INIT(heap, mm128_t, heap_lt)

typedef struct {
	uint32_t n;
	uint32_t q_pos, q_span;
	uint32_t seg_id:31, is_tandem:1;
	const uint64_t *cr;
} mm_match_t;

static mm_match_t *collect_matches(void *km, int *_n_m, int max_occ, const mm_idx_t *mi, const mm128_v *mv, int64_t *n_a, int *rep_len, int *n_mini_pos, uint64_t **mini_pos)
{
	int rep_st = 0, rep_en = 0, n_m;
	size_t i;
	mm_match_t *m;
	*n_mini_pos = 0;
	*mini_pos = (uint64_t*)kmalloc(km, mv->n * sizeof(uint64_t));
	m = (mm_match_t*)kmalloc(km, mv->n * sizeof(mm_match_t));
	for (i = 0, n_m = 0, *rep_len = 0, *n_a = 0; i < mv->n; ++i) {
		const uint64_t *cr;
		mm128_t *p = &mv->a[i];
		uint32_t q_pos = (uint32_t)p->y, q_span = p->x & 0xff;
		int t;
		cr = mm_idx_get(mi, p->x>>8, &t);
		if (t >= max_occ) {
			int en = (q_pos >> 1) + 1, st = en - q_span;
			if (st > rep_en) {
				*rep_len += rep_en - rep_st;
				rep_st = st, rep_en = en;
			} else rep_en = en;
		} else {
			mm_match_t *q = &m[n_m++];
			q->q_pos = q_pos, q->q_span = q_span, q->cr = cr, q->n = t, q->seg_id = p->y >> 32;
			q->is_tandem = 0;
			if (i > 0 && p->x>>8 == mv->a[i - 1].x>>8) q->is_tandem = 1;
			if (i < mv->n - 1 && p->x>>8 == mv->a[i + 1].x>>8) q->is_tandem = 1;
			*n_a += q->n;
			(*mini_pos)[(*n_mini_pos)++] = (uint64_t)q_span<<32 | q_pos>>1;
		}
	}
	*rep_len += rep_en - rep_st;
	*_n_m = n_m;
	return m;
}

static inline int skip_seed(int flag, uint64_t r, const mm_match_t *q, const char *qname, int qlen, const mm_idx_t *mi, int *is_self)
{
	*is_self = 0;
	if (qname && (flag & (MM_F_NO_DIAG|MM_F_NO_DUAL))) {
		const mm_idx_seq_t *s = &mi->seq[r>>32];
		int cmp;
		cmp = strcmp(qname, s->name);
		if ((flag&MM_F_NO_DIAG) && cmp == 0 && (int)s->len == qlen) {
			if ((uint32_t)r>>1 == (q->q_pos>>1)) return 1; // avoid the diagnonal anchors
			if ((r&1) == (q->q_pos&1)) *is_self = 1; // this flag is used to avoid spurious extension on self chain
		}
		if ((flag&MM_F_NO_DUAL) && cmp > 0) // all-vs-all mode: map once
			return 1;
	}
	if (flag & (MM_F_FOR_ONLY|MM_F_REV_ONLY)) {
		if ((r&1) == (q->q_pos&1)) { // forward strand
			if (flag & MM_F_REV_ONLY) return 1;
		} else {
			if (flag & MM_F_FOR_ONLY) return 1;
		}
	}
	return 0;
}

static mm128_t *collect_seed_hits_heap(void *km, const mm_mapopt_t *opt, int max_occ, const mm_idx_t *mi, const char *qname, const mm128_v *mv, int qlen, int64_t *n_a, int *rep_len,
								  int *n_mini_pos, uint64_t **mini_pos)
{
	int i, n_m, heap_size = 0;
	int64_t j, n_for = 0, n_rev = 0;
	mm_match_t *m;
	mm128_t *a, *heap;

	m = collect_matches(km, &n_m, max_occ, mi, mv, n_a, rep_len, n_mini_pos, mini_pos);

	heap = (mm128_t*)kmalloc(km, n_m * sizeof(mm128_t));
	a = (mm128_t*)kmalloc(km, *n_a * sizeof(mm128_t));

	for (i = 0, heap_size = 0; i < n_m; ++i) {
		if (m[i].n > 0) {
			heap[heap_size].x = m[i].cr[0];
			heap[heap_size].y = (uint64_t)i<<32;
			++heap_size;
		}
	}
	ks_heapmake_heap(heap_size, heap);
	while (heap_size > 0) {
		mm_match_t *q = &m[heap->y>>32];
		mm128_t *p;
		uint64_t r = heap->x;
		int32_t is_self, rpos = (uint32_t)r >> 1;
		if (!skip_seed(opt->flag, r, q, qname, qlen, mi, &is_self)) {
			if ((r&1) == (q->q_pos&1)) { // forward strand
				p = &a[n_for++];
				p->x = (r&0xffffffff00000000ULL) | rpos;
				p->y = (uint64_t)q->q_span << 32 | q->q_pos >> 1;
			} else { // reverse strand
				p = &a[(*n_a) - (++n_rev)];
				p->x = 1ULL<<63 | (r&0xffffffff00000000ULL) | rpos;
				p->y = (uint64_t)q->q_span << 32 | (qlen - ((q->q_pos>>1) + 1 - q->q_span) - 1);
			}
			p->y |= (uint64_t)q->seg_id << MM_SEED_SEG_SHIFT;
			if (q->is_tandem) p->y |= MM_SEED_TANDEM;
			if (is_self) p->y |= MM_SEED_SELF;
		}
		// update the heap
		if ((uint32_t)heap->y < q->n - 1) {
			++heap[0].y;
			heap[0].x = m[heap[0].y>>32].cr[(uint32_t)heap[0].y];
		} else {
			heap[0] = heap[heap_size - 1];
			--heap_size;
		}
		ks_heapdown_heap(0, heap_size, heap);
	}
	kfree(km, m);
	kfree(km, heap);

	// reverse anchors on the reverse strand, as they are in the descending order
	for (j = 0; j < n_rev>>1; ++j) {
		mm128_t t = a[(*n_a) - 1 - j];
		a[(*n_a) - 1 - j] = a[(*n_a) - (n_rev - j)];
		a[(*n_a) - (n_rev - j)] = t;
	}
	if (*n_a > n_for + n_rev) {
		memmove(a + n_for, a + (*n_a) - n_rev, n_rev * sizeof(mm128_t));
		*n_a = n_for + n_rev;
	}
	return a;
}

static mm128_t *collect_seed_hits(void *km, const mm_mapopt_t *opt, int max_occ, const mm_idx_t *mi, const char *qname, const mm128_v *mv, int qlen, int64_t *n_a, int *rep_len,
								  int *n_mini_pos, uint64_t **mini_pos)
{
	int i, n_m;
	mm_match_t *m;
	mm128_t *a;
	m = collect_matches(km, &n_m, max_occ, mi, mv, n_a, rep_len, n_mini_pos, mini_pos);
	a = (mm128_t*)kmalloc(km, *n_a * sizeof(mm128_t));
	for (i = 0, *n_a = 0; i < n_m; ++i) {
		mm_match_t *q = &m[i];
		const uint64_t *r = q->cr;
		uint32_t k;
		for (k = 0; k < q->n; ++k) {
			int32_t is_self, rpos = (uint32_t)r[k] >> 1;
			mm128_t *p;
			if (skip_seed(opt->flag, r[k], q, qname, qlen, mi, &is_self)) continue;
			p = &a[(*n_a)++];
			if ((r[k]&1) == (q->q_pos&1)) { // forward strand
				p->x = (r[k]&0xffffffff00000000ULL) | rpos;
				p->y = (uint64_t)q->q_span << 32 | q->q_pos >> 1;
			} else { // reverse strand
				p->x = 1ULL<<63 | (r[k]&0xffffffff00000000ULL) | rpos;
				p->y = (uint64_t)q->q_span << 32 | (qlen - ((q->q_pos>>1) + 1 - q->q_span) - 1);
			}
			p->y |= (uint64_t)q->seg_id << MM_SEED_SEG_SHIFT;
			if (q->is_tandem) p->y |= MM_SEED_TANDEM;
			if (is_self) p->y |= MM_SEED_SELF;
		}
	}
	kfree(km, m);
	radix_sort_128x(a, a + (*n_a));
	return a;
}

static void chain_post(const mm_mapopt_t *opt, int max_chain_gap_ref, const mm_idx_t *mi, void *km, int qlen, int n_segs, const int *qlens, int *n_regs, mm_reg1_t *regs, mm128_t *a)
{
	if (!(opt->flag & MM_F_ALL_CHAINS)) { // don't choose primary mapping(s)
		mm_set_parent(km, opt->mask_level, opt->mask_len, *n_regs, regs, opt->a * 2 + opt->b, opt->flag&MM_F_HARD_MLEVEL, opt->alt_drop);
		if (n_segs <= 1) mm_select_sub(km, opt->pri_ratio, mi->k*2, opt->best_n, n_regs, regs);
		else mm_select_sub_multi(km, opt->pri_ratio, 0.2f, 0.7f, max_chain_gap_ref, mi->k*2, opt->best_n, n_segs, qlens, n_regs, regs);
		if (!(opt->flag & (MM_F_SPLICE|MM_F_SR|MM_F_NO_LJOIN))) // long join not working well without primary chains
			mm_join_long(km, opt, qlen, n_regs, regs, a);
	}
}

static mm_reg1_t *align_regs(const mm_mapopt_t *opt, const mm_idx_t *mi, void *km, int qlen, const char *seq, int *n_regs, mm_reg1_t *regs, mm128_t *a)
{
	if (!(opt->flag & MM_F_CIGAR)) return regs;
	regs = mm_align_skeleton(km, opt, mi, qlen, seq, n_regs, regs, a); // this calls mm_filter_regs()
	if (!(opt->flag & MM_F_ALL_CHAINS)) { // don't choose primary mapping(s)
		mm_set_parent(km, opt->mask_level, opt->mask_len, *n_regs, regs, opt->a * 2 + opt->b, opt->flag&MM_F_HARD_MLEVEL, opt->alt_drop);
		mm_select_sub(km, opt->pri_ratio, mi->k*2, opt->best_n, n_regs, regs);
		mm_set_sam_pri(*n_regs, regs);
	}
	return regs;
}

void mm_map_frag(const mm_idx_t *mi, int n_segs, const int *qlens, const char **seqs, int *n_regs, mm_reg1_t **regs, mm_tbuf_t *b, const mm_mapopt_t *opt, const char *qname)
{
	int i, j, rep_len, qlen_sum, n_regs0, n_mini_pos;
	int max_chain_gap_qry, max_chain_gap_ref, is_splice = !!(opt->flag & MM_F_SPLICE), is_sr = !!(opt->flag & MM_F_SR);
	uint32_t hash;
	int64_t n_a;
	uint64_t *u, *mini_pos;
	mm128_t *a;
	mm128_v mv = {0,0,0};
	mm_reg1_t *regs0;
	km_stat_t kmst;

	for (i = 0, qlen_sum = 0; i < n_segs; ++i)
		qlen_sum += qlens[i], n_regs[i] = 0, regs[i] = 0;

	if (qlen_sum == 0 || n_segs <= 0 || n_segs > MM_MAX_SEG) return;
	if (opt->max_qlen > 0 && qlen_sum > opt->max_qlen) return;

	hash  = qname? __ac_X31_hash_string(qname) : 0;
	hash ^= __ac_Wang_hash(qlen_sum) + __ac_Wang_hash(opt->seed);
	hash  = __ac_Wang_hash(hash);

	collect_minimizers(b->km, opt, mi, n_segs, qlens, seqs, &mv);
	if (opt->flag & MM_F_HEAP_SORT) a = collect_seed_hits_heap(b->km, opt, opt->mid_occ, mi, qname, &mv, qlen_sum, &n_a, &rep_len, &n_mini_pos, &mini_pos);
	else a = collect_seed_hits(b->km, opt, opt->mid_occ, mi, qname, &mv, qlen_sum, &n_a, &rep_len, &n_mini_pos, &mini_pos);

	if (mm_dbg_flag & MM_DBG_PRINT_SEED) {
		fprintf(stderr, "RS\t%d\n", rep_len);
		for (i = 0; i < n_a; ++i)
			fprintf(stderr, "SD\t%s\t%d\t%c\t%d\t%d\t%d\n", mi->seq[a[i].x<<1>>33].name, (int32_t)a[i].x, "+-"[a[i].x>>63], (int32_t)a[i].y, (int32_t)(a[i].y>>32&0xff),
					i == 0? 0 : ((int32_t)a[i].y - (int32_t)a[i-1].y) - ((int32_t)a[i].x - (int32_t)a[i-1].x));
	}

	// set max chaining gap on the query and the reference sequence
	if (is_sr)
		max_chain_gap_qry = qlen_sum > opt->max_gap? qlen_sum : opt->max_gap;
	else max_chain_gap_qry = opt->max_gap;
	if (opt->max_gap_ref > 0) {
		max_chain_gap_ref = opt->max_gap_ref; // always honor mm_mapopt_t::max_gap_ref if set
	} else if (opt->max_frag_len > 0) {
		max_chain_gap_ref = opt->max_frag_len - qlen_sum;
		if (max_chain_gap_ref < opt->max_gap) max_chain_gap_ref = opt->max_gap;
	} else max_chain_gap_ref = opt->max_gap;

	a = mm_chain_dp(max_chain_gap_ref, max_chain_gap_qry, opt->bw, opt->max_chain_skip, opt->max_chain_iter, opt->min_cnt, opt->min_chain_score, opt->chain_gap_scale, is_splice, n_segs, n_a, a, &n_regs0, &u, b->km);

	if (opt->max_occ > opt->mid_occ && rep_len > 0) {
		int rechain = 0;
		if (n_regs0 > 0) { // test if the best chain has all the segments
			int n_chained_segs = 1, max = 0, max_i = -1, max_off = -1, off = 0;
			for (i = 0; i < n_regs0; ++i) { // find the best chain
				if (max < (int)(u[i]>>32)) max = u[i]>>32, max_i = i, max_off = off;
				off += (uint32_t)u[i];
			}
			for (i = 1; i < (int32_t)u[max_i]; ++i) // count the number of segments in the best chain
				if ((a[max_off+i].y&MM_SEED_SEG_MASK) != (a[max_off+i-1].y&MM_SEED_SEG_MASK))
					++n_chained_segs;
			if (n_chained_segs < n_segs)
				rechain = 1;
		} else rechain = 1;
		if (rechain) { // redo chaining with a higher max_occ threshold
			kfree(b->km, a);
			kfree(b->km, u);
			kfree(b->km, mini_pos);
			if (opt->flag & MM_F_HEAP_SORT) a = collect_seed_hits_heap(b->km, opt, opt->max_occ, mi, qname, &mv, qlen_sum, &n_a, &rep_len, &n_mini_pos, &mini_pos);
			else a = collect_seed_hits(b->km, opt, opt->max_occ, mi, qname, &mv, qlen_sum, &n_a, &rep_len, &n_mini_pos, &mini_pos);
			a = mm_chain_dp(max_chain_gap_ref, max_chain_gap_qry, opt->bw, opt->max_chain_skip, opt->max_chain_iter, opt->min_cnt, opt->min_chain_score, opt->chain_gap_scale, is_splice, n_segs, n_a, a, &n_regs0, &u, b->km);
		}
	}
	b->frag_gap = max_chain_gap_ref;
	b->rep_len = rep_len;

	regs0 = mm_gen_regs(b->km, hash, qlen_sum, n_regs0, u, a);
	if (mi->n_alt) {
		mm_mark_alt(mi, n_regs0, regs0);
		mm_hit_sort(b->km, &n_regs0, regs0, opt->alt_drop); // this step can be merged into mm_gen_regs(); will do if this shows up in profile
	}

	if (mm_dbg_flag & MM_DBG_PRINT_SEED)
		for (j = 0; j < n_regs0; ++j)
			for (i = regs0[j].as; i < regs0[j].as + regs0[j].cnt; ++i)
				fprintf(stderr, "CN\t%d\t%s\t%d\t%c\t%d\t%d\t%d\n", j, mi->seq[a[i].x<<1>>33].name, (int32_t)a[i].x, "+-"[a[i].x>>63], (int32_t)a[i].y, (int32_t)(a[i].y>>32&0xff),
						i == regs0[j].as? 0 : ((int32_t)a[i].y - (int32_t)a[i-1].y) - ((int32_t)a[i].x - (int32_t)a[i-1].x));

	chain_post(opt, max_chain_gap_ref, mi, b->km, qlen_sum, n_segs, qlens, &n_regs0, regs0, a);
	if (!is_sr) mm_est_err(mi, qlen_sum, n_regs0, regs0, a, n_mini_pos, mini_pos);

	if (n_segs == 1) { // uni-segment
		regs0 = align_regs(opt, mi, b->km, qlens[0], seqs[0], &n_regs0, regs0, a);
		mm_set_mapq(b->km, n_regs0, regs0, opt->min_chain_score, opt->a, rep_len, is_sr);
		n_regs[0] = n_regs0, regs[0] = regs0;
	} else { // multi-segment
		mm_seg_t *seg;
		seg = mm_seg_gen(b->km, hash, n_segs, qlens, n_regs0, regs0, n_regs, regs, a); // split fragment chain to separate segment chains
		free(regs0);
		for (i = 0; i < n_segs; ++i) {
			mm_set_parent(b->km, opt->mask_level, opt->mask_len, n_regs[i], regs[i], opt->a * 2 + opt->b, opt->flag&MM_F_HARD_MLEVEL, opt->alt_drop); // update mm_reg1_t::parent
			regs[i] = align_regs(opt, mi, b->km, qlens[i], seqs[i], &n_regs[i], regs[i], seg[i].a);
			mm_set_mapq(b->km, n_regs[i], regs[i], opt->min_chain_score, opt->a, rep_len, is_sr);
		}
		mm_seg_free(b->km, n_segs, seg);
		if (n_segs == 2 && opt->pe_ori >= 0 && (opt->flag&MM_F_CIGAR))
			mm_pair(b->km, max_chain_gap_ref, opt->pe_bonus, opt->a * 2 + opt->b, opt->a, qlens, n_regs, regs); // pairing
	}

	kfree(b->km, mv.a);
	kfree(b->km, a);
	kfree(b->km, u);
	kfree(b->km, mini_pos);

	if (b->km) {
		km_stat(b->km, &kmst);
		if (mm_dbg_flag & MM_DBG_PRINT_QNAME)
			fprintf(stderr, "QM\t%s\t%d\tcap=%ld,nCore=%ld,largest=%ld\n", qname, qlen_sum, kmst.capacity, kmst.n_cores, kmst.largest);
		assert(kmst.n_blocks == kmst.n_cores); // otherwise, there is a memory leak
		if (kmst.largest > 1U<<28) {
			km_destroy(b->km);
			b->km = km_init();
		}
	}
}

mm_reg1_t *mm_map(const mm_idx_t *mi, int qlen, const char *seq, int *n_regs, mm_tbuf_t *b, const mm_mapopt_t *opt, const char *qname)
{
	mm_reg1_t *regs;
	mm_map_frag(mi, 1, &qlen, &seq, n_regs, &regs, b, opt, qname);
	return regs;
}

/**************************
 * Multi-threaded mapping *
 **************************/

typedef struct {
	int n_processed, n_threads, n_fp;
	int64_t mini_batch_size;
	const mm_mapopt_t *opt;
	mm_bseq_file_t **fp;
	const mm_idx_t *mi;
	kstring_t str;

	int n_parts;
	uint32_t *rid_shift;
	FILE *fp_split, **fp_parts;
} pipeline_t;

typedef struct {
	const pipeline_t *p;
    int n_seq, n_frag;
	mm_bseq1_t *seq;
	int *n_reg, *seg_off, *n_seg, *rep_len, *frag_gap;
	mm_reg1_t **reg;
	mm_tbuf_t **buf;
} step_t;

static void worker_for(void *_data, long i, int tid) // kt_for() callback
{
    step_t *s = (step_t*)_data;
	int qlens[MM_MAX_SEG], j, off = s->seg_off[i], pe_ori = s->p->opt->pe_ori;
	const char *qseqs[MM_MAX_SEG];
	mm_tbuf_t *b = s->buf[tid];
	assert(s->n_seg[i] <= MM_MAX_SEG);
	if (mm_dbg_flag & MM_DBG_PRINT_QNAME)
		fprintf(stderr, "QR\t%s\t%d\t%d\n", s->seq[off].name, tid, s->seq[off].l_seq);
	for (j = 0; j < s->n_seg[i]; ++j) {
		if (s->n_seg[i] == 2 && ((j == 0 && (pe_ori>>1&1)) || (j == 1 && (pe_ori&1))))
			mm_revcomp_bseq(&s->seq[off + j]);
		qlens[j] = s->seq[off + j].l_seq;
		qseqs[j] = s->seq[off + j].seq;
	}
	if (s->p->opt->flag & MM_F_INDEPEND_SEG) {
		for (j = 0; j < s->n_seg[i]; ++j) {
			mm_map_frag(s->p->mi, 1, &qlens[j], &qseqs[j], &s->n_reg[off+j], &s->reg[off+j], b, s->p->opt, s->seq[off+j].name);
			s->rep_len[off + j] = b->rep_len;
			s->frag_gap[off + j] = b->frag_gap;
		}
	} else {
		mm_map_frag(s->p->mi, s->n_seg[i], qlens, qseqs, &s->n_reg[off], &s->reg[off], b, s->p->opt, s->seq[off].name);
		for (j = 0; j < s->n_seg[i]; ++j) {
			s->rep_len[off + j] = b->rep_len;
			s->frag_gap[off + j] = b->frag_gap;
		}
	}
	for (j = 0; j < s->n_seg[i]; ++j) // flip the query strand and coordinate to the original read strand
		if (s->n_seg[i] == 2 && ((j == 0 && (pe_ori>>1&1)) || (j == 1 && (pe_ori&1)))) {
			int k, t;
			mm_revcomp_bseq(&s->seq[off + j]);
			for (k = 0; k < s->n_reg[off + j]; ++k) {
				mm_reg1_t *r = &s->reg[off + j][k];
				t = r->qs;
				r->qs = qlens[j] - r->qe;
				r->qe = qlens[j] - t;
				r->rev = !r->rev;
			}
		}
}

static void merge_hits(step_t *s)
{
	int f, i, k0, k, max_seg = 0, *n_reg_part, *rep_len_part, *frag_gap_part, *qlens;
	void *km;
	FILE **fp = s->p->fp_parts;
	const mm_mapopt_t *opt = s->p->opt;

	km = km_init();
	for (f = 0; f < s->n_frag; ++f)
		max_seg = max_seg > s->n_seg[f]? max_seg : s->n_seg[f];
	qlens = CALLOC(int, max_seg + s->p->n_parts * 3);
	n_reg_part = qlens + max_seg;
	rep_len_part = n_reg_part + s->p->n_parts;
	frag_gap_part = rep_len_part + s->p->n_parts;
	for (f = 0, k = k0 = 0; f < s->n_frag; ++f) {
		k0 = k;
		for (i = 0; i < s->n_seg[f]; ++i, ++k) {
			int j, l, t, rep_len = 0;
			qlens[i] = s->seq[k].l_seq;
			for (j = 0, s->n_reg[k] = 0; j < s->p->n_parts; ++j) {
				mm_err_fread(&n_reg_part[j],    sizeof(int), 1, fp[j]);
				mm_err_fread(&rep_len_part[j],  sizeof(int), 1, fp[j]);
				mm_err_fread(&frag_gap_part[j], sizeof(int), 1, fp[j]);
				s->n_reg[k] += n_reg_part[j];
				if (rep_len < rep_len_part[j])
					rep_len = rep_len_part[j];
			}
			s->reg[k] = CALLOC(mm_reg1_t, s->n_reg[k]);
			for (j = 0, l = 0; j < s->p->n_parts; ++j) {
				for (t = 0; t < n_reg_part[j]; ++t, ++l) {
					mm_reg1_t *r = &s->reg[k][l];
					uint32_t capacity;
					mm_err_fread(r, sizeof(mm_reg1_t), 1, fp[j]);
					r->rid += s->p->rid_shift[j];
					if (opt->flag & MM_F_CIGAR) {
						mm_err_fread(&capacity, 4, 1, fp[j]);
						r->p = (mm_extra_t*)calloc(capacity, 4);
						r->p->capacity = capacity;
						mm_err_fread(r->p, r->p->capacity, 4, fp[j]);
					}
				}
			}
			mm_hit_sort(km, &s->n_reg[k], s->reg[k], opt->alt_drop);
			mm_set_parent(km, opt->mask_level, opt->mask_len, s->n_reg[k], s->reg[k], opt->a * 2 + opt->b, opt->flag&MM_F_HARD_MLEVEL, opt->alt_drop);
			if (!(opt->flag & MM_F_ALL_CHAINS)) {
				mm_select_sub(km, opt->pri_ratio, s->p->mi->k*2, opt->best_n, &s->n_reg[k], s->reg[k]);
				mm_set_sam_pri(s->n_reg[k], s->reg[k]);
			}
			mm_set_mapq(km, s->n_reg[k], s->reg[k], opt->min_chain_score, opt->a, rep_len, !!(opt->flag & MM_F_SR));
		}
		if (s->n_seg[f] == 2 && opt->pe_ori >= 0 && (opt->flag&MM_F_CIGAR))
			mm_pair(km, frag_gap_part[0], opt->pe_bonus, opt->a * 2 + opt->b, opt->a, qlens, &s->n_reg[k0], &s->reg[k0]);
	}
	free(qlens);
	km_destroy(km);
}

static void *worker_pipeline(void *shared, int step, void *in)
{
	int i, j, k;
    pipeline_t *p = (pipeline_t*)shared;
    if (step == 0) { // step 0: read sequences
		int with_qual = (!!(p->opt->flag & MM_F_OUT_SAM) && !(p->opt->flag & MM_F_NO_QUAL));
		int with_comment = !!(p->opt->flag & MM_F_COPY_COMMENT);
		int frag_mode = (p->n_fp > 1 || !!(p->opt->flag & MM_F_FRAG_MODE));
        step_t *s;
        s = (step_t*)calloc(1, sizeof(step_t));
		if (p->n_fp > 1) s->seq = mm_bseq_read_frag2(p->n_fp, p->fp, p->mini_batch_size, with_qual, with_comment, &s->n_seq);
		else s->seq = mm_bseq_read3(p->fp[0], p->mini_batch_size, with_qual, with_comment, frag_mode, &s->n_seq);
		if (s->seq) {
			s->p = p;
			for (i = 0; i < s->n_seq; ++i)
				s->seq[i].rid = p->n_processed++;
			s->buf = (mm_tbuf_t**)calloc(p->n_threads, sizeof(mm_tbuf_t*));
			for (i = 0; i < p->n_threads; ++i)
				s->buf[i] = mm_tbuf_init();
			s->n_reg = (int*)calloc(5 * s->n_seq, sizeof(int));
			s->seg_off = s->n_reg + s->n_seq; // seg_off, n_seg, rep_len and frag_gap are allocated together with n_reg
			s->n_seg = s->seg_off + s->n_seq;
			s->rep_len = s->n_seg + s->n_seq;
			s->frag_gap = s->rep_len + s->n_seq;
			s->reg = (mm_reg1_t**)calloc(s->n_seq, sizeof(mm_reg1_t*));
			for (i = 1, j = 0; i <= s->n_seq; ++i)
				if (i == s->n_seq || !frag_mode || !mm_qname_same(s->seq[i-1].name, s->seq[i].name)) {
					s->n_seg[s->n_frag] = i - j;
					s->seg_off[s->n_frag++] = j;
					j = i;
				}
			return s;
		} else free(s);
    } else if (step == 1) { // step 1: map
		if (p->n_parts > 0) merge_hits((step_t*)in);
		else kt_for(p->n_threads, worker_for, in, ((step_t*)in)->n_frag);
		return in;
    } else if (step == 2) { // step 2: output
		void *km = 0;
        step_t *s = (step_t*)in;
		const mm_idx_t *mi = p->mi;
		for (i = 0; i < p->n_threads; ++i) mm_tbuf_destroy(s->buf[i]);
		free(s->buf);
		if ((p->opt->flag & MM_F_OUT_CS) && !(mm_dbg_flag & MM_DBG_NO_KALLOC)) km = km_init();
		for (k = 0; k < s->n_frag; ++k) {
			int seg_st = s->seg_off[k], seg_en = s->seg_off[k] + s->n_seg[k];
			for (i = seg_st; i < seg_en; ++i) {
				mm_bseq1_t *t = &s->seq[i];
				if (p->opt->split_prefix && p->n_parts == 0) { // then write to temporary files
					mm_err_fwrite(&s->n_reg[i],    sizeof(int), 1, p->fp_split);
					mm_err_fwrite(&s->rep_len[i],  sizeof(int), 1, p->fp_split);
					mm_err_fwrite(&s->frag_gap[i], sizeof(int), 1, p->fp_split);
					for (j = 0; j < s->n_reg[i]; ++j) {
						mm_reg1_t *r = &s->reg[i][j];
						mm_err_fwrite(r, sizeof(mm_reg1_t), 1, p->fp_split);
						if (p->opt->flag & MM_F_CIGAR) {
							mm_err_fwrite(&r->p->capacity, 4, 1, p->fp_split);
							mm_err_fwrite(r->p, r->p->capacity, 4, p->fp_split);
						}
					}
				} else if (s->n_reg[i] > 0) { // the query has at least one hit
					for (j = 0; j < s->n_reg[i]; ++j) {
						mm_reg1_t *r = &s->reg[i][j];
						assert(!r->sam_pri || r->id == r->parent);
						if ((p->opt->flag & MM_F_NO_PRINT_2ND) && r->id != r->parent)
							continue;
						if (p->opt->flag & MM_F_OUT_SAM)
							mm_write_sam3(&p->str, mi, t, i - seg_st, j, s->n_seg[k], &s->n_reg[seg_st], (const mm_reg1_t*const*)&s->reg[seg_st], km, p->opt->flag, s->rep_len[i]);
						else
							mm_write_paf3(&p->str, mi, t, r, km, p->opt->flag, s->rep_len[i]);
						//mm_err_puts(p->str.s);
						if (strcmp(t->refID, "chr1")==0) //t->beginPos
						{
						    if (t->beginPos < 49850124 )  
						    {arrow_builders_append(1, t->beginPos, p->str.s);counts1++;}
						    else if (t->beginPos >= 49850124 && t->beginPos < 99700248) 
						    {arrow_builders_append(110, t->beginPos, p->str.s);counts1_1++;}
						    else if (t->beginPos >= 99700248 && t->beginPos < 149550372) 
						    {arrow_builders_append(120, t->beginPos, p->str.s);counts1_2++;}
						    else if (t->beginPos >= 149550372 && t->beginPos < 199400496)
						    {arrow_builders_append(130, t->beginPos, p->str.s);counts1_3++;}
						    else {arrow_builders_append(140, t->beginPos, p->str.s);counts1_4++;}
						}
						else if (strcmp(t->refID, "chr2")==0)
						{
						    if (t->beginPos < 48438705 )  
						    {arrow_builders_append(2, t->beginPos, p->str.s);counts2++;}
						    else if (t->beginPos >= 48438705 && t->beginPos < 96877410) 
						    {arrow_builders_append(210, t->beginPos, p->str.s);counts2_1++;}
						    else if (t->beginPos >= 96877410 && t->beginPos < 145316115) 
						    {arrow_builders_append(220, t->beginPos, p->str.s);counts2_2++;}
						    else if (t->beginPos >= 145316115 && t->beginPos < 193754820)
						    {arrow_builders_append(230, t->beginPos, p->str.s);counts2_3++;}
						    else {arrow_builders_append(240, t->beginPos, p->str.s);counts2_4++;}
						}
						else if (strcmp(t->refID, "chr3")==0)
						{
						    if (t->beginPos < 49573889 )  
						    {arrow_builders_append(3, t->beginPos, p->str.s);counts3++;}
						    else if (t->beginPos >= 49573889 && t->beginPos < 99147778) 
						    {arrow_builders_append(31, t->beginPos, p->str.s);counts3_1++;}
						    else if (t->beginPos >= 99147778 && t->beginPos < 148721667) 
						    {arrow_builders_append(32, t->beginPos, p->str.s);counts3_2++;}
						    else {arrow_builders_append(33, t->beginPos, p->str.s);counts3_3++;}
						}
						else if (strcmp(t->refID, "chr4")==0)
						{
						    if (t->beginPos < 49573889 )  
						    {arrow_builders_append(4, t->beginPos, p->str.s);counts4++;}
						    else if (t->beginPos >= 49573889 && t->beginPos < 99147778) 
						    {arrow_builders_append(41, t->beginPos, p->str.s);counts4_1++;}
						    else if (t->beginPos >= 99147779 && t->beginPos < 148721667) 
						    {arrow_builders_append(42, t->beginPos, p->str.s);counts4_2++;}
						    else {arrow_builders_append(43, t->beginPos, p->str.s);counts4_3++;}
						}
						else if (strcmp(t->refID, "chr5")==0)
						{
						    if (t->beginPos < 49573889 )  
						    {arrow_builders_append(5, t->beginPos, p->str.s);counts5++;}
						    else if (t->beginPos >= 49573889 && t->beginPos < 99147778) 
						    {arrow_builders_append(51, t->beginPos, p->str.s);counts5_1++;}
						    else if (t->beginPos >= 99147778 && t->beginPos < 148721667) 
						    {arrow_builders_append(52, t->beginPos, p->str.s);counts5_2++;}
						    else {arrow_builders_append(53, t->beginPos, p->str.s);counts5_3++;}
						}
						else if (strcmp(t->refID, "chr6")==0)
						{
						    if (t->beginPos < 42701494 )  
						    {arrow_builders_append(6, t->beginPos, p->str.s);counts6++;}
						    else if (t->beginPos >= 42701494 && t->beginPos < 85402988) 
						    {arrow_builders_append(61, t->beginPos, p->str.s);counts6_1++;}
						    else if (t->beginPos >= 85402988 && t->beginPos < 128104482) 
						    {arrow_builders_append(62, t->beginPos, p->str.s);counts6_2++;}
						    else {arrow_builders_append(63, t->beginPos, p->str.s);counts6_3++;}
						}
						else if (strcmp(t->refID, "chr7")==0)
						{
						    if (t->beginPos < 53115324 )  
						    {arrow_builders_append(7, t->beginPos, p->str.s);counts7++;}
						    else if (t->beginPos >= 53115324 && t->beginPos < 106230648) 
						    {arrow_builders_append(71, t->beginPos, p->str.s);counts7_1++;}
						    else {arrow_builders_append(72, t->beginPos, p->str.s);counts7_2++;}
						}
						else if (strcmp(t->refID, "chr8")==0)
						{
						    if (t->beginPos < 48379545 )  
						    {arrow_builders_append(8, t->beginPos, p->str.s);counts8++;}
						    else if (t->beginPos >= 48379545 && t->beginPos < 96759090) 
						    {arrow_builders_append(81, t->beginPos, p->str.s);counts8_1++;}
						    else {arrow_builders_append(82, t->beginPos, p->str.s);counts8_2++;}
						}
						else if (strcmp(t->refID, "chr9")==0)
						{
						    if (t->beginPos < 46131572 )  
						    {arrow_builders_append(9, t->beginPos, p->str.s);counts9++;}
						    else if (t->beginPos >= 46131572 && t->beginPos < 92263144) 
						    {arrow_builders_append(91, t->beginPos, p->str.s);counts9_1++;}
						    else {arrow_builders_append(92, t->beginPos, p->str.s);counts9_2++;}
						}
						else if (strcmp(t->refID, "chr10")==0)
						{
						    if (t->beginPos < 44599140 )  
						    {arrow_builders_append(10, t->beginPos, p->str.s);counts10++;}
						    else if (t->beginPos >= 44599140 && t->beginPos < 89198280) 
						    {arrow_builders_append(101, t->beginPos, p->str.s);counts10_1++;}
						    else {arrow_builders_append(102, t->beginPos, p->str.s);counts10_2++;}
						}
						else if (strcmp(t->refID, "chr11")==0)
						{
						    if (t->beginPos < 45028874 )  
						    {arrow_builders_append(11, t->beginPos, p->str.s);counts11++;}
						    else if (t->beginPos >= 45028874 && t->beginPos < 90057748) 
						    {arrow_builders_append(111, t->beginPos, p->str.s);counts11_1++;}
						    else {arrow_builders_append(112, t->beginPos, p->str.s);counts11_2++;}
						}
						else if (strcmp(t->refID, "chr12")==0)
						{
						    if (t->beginPos < 44425103 )  
						    {arrow_builders_append(12, t->beginPos, p->str.s);counts12++;}
						    else if (t->beginPos >= 44425103 && t->beginPos < 88850206) 
						    {arrow_builders_append(121, t->beginPos, p->str.s);counts12_1++;}
						    else {arrow_builders_append(122, t->beginPos, p->str.s);counts12_2++;}
						}
						else if (strcmp(t->refID, "chr13")==0)
						{
						    if (t->beginPos < 57182164 )  
						    {arrow_builders_append(13, t->beginPos, p->str.s);counts13++;}
						    else {arrow_builders_append(131, t->beginPos, p->str.s);counts13_1++;}
						}
						else if (strcmp(t->refID, "chr14")==0)
						{
						    if (t->beginPos < 53180292)  
						    {arrow_builders_append(14, t->beginPos, p->str.s);counts14++;}
						    else {arrow_builders_append(141, t->beginPos, p->str.s);counts14_1++;}
						}
						else if (strcmp(t->refID, "chr15")==0)
						{
						    if (t->beginPos < 50995594 )  
						    {arrow_builders_append(15, t->beginPos, p->str.s);counts15++;}
						    else {arrow_builders_append(151, t->beginPos, p->str.s);counts15_1++;}
						}
						else if (strcmp(t->refID, "chr16")==0)
						{
						    if (t->beginPos < 45169172 )  
						    {arrow_builders_append(16, t->beginPos, p->str.s);counts16++;}
						    else {arrow_builders_append(161, t->beginPos, p->str.s);counts16_1++;}
						}
						else if (strcmp(t->refID, "chr17")==0)
						{
						    if (t->beginPos < 41628720 )  
						    {arrow_builders_append(17, t->beginPos, p->str.s);counts17++;}
						    else {arrow_builders_append(171, t->beginPos, p->str.s);counts17_1++;}
						}
						else if (strcmp(t->refID, "chr18")==0)
						{
						    if (t->beginPos < 40186642 )  
						    {arrow_builders_append(18, t->beginPos, p->str.s);counts18++;}
						    else {arrow_builders_append(181, t->beginPos, p->str.s);counts18_1++;}
						}
						else if (strcmp(t->refID, "chr19")==0)
						{
						    arrow_builders_append(19, t->beginPos, p->str.s);counts19++;
						 }
						else if (strcmp(t->refID, "chr20")==0)
						{
						    arrow_builders_append(20, t->beginPos, p->str.s);counts20++;
						 }
						else if (strcmp(t->refID, "chr21")==0)
						{
						    arrow_builders_append(21, t->beginPos, p->str.s);counts21++;
						 }
						else if (strcmp(t->refID, "chr22")==0)
						{
						    arrow_builders_append(22, t->beginPos, p->str.s);counts22++;
						 }
						else if (strcmp(t->refID, "chrX")==0)
						{
						    if (t->beginPos < 52013631 )  
						    {arrow_builders_append(23, t->beginPos, p->str.s);countsX++;}
						    else if (t->beginPos >= 52013631 && t->beginPos < 104027262) 
						    {arrow_builders_append(231, t->beginPos, p->str.s);countsX_1++;}
						    else {arrow_builders_append(232, t->beginPos, p->str.s);countsX_2++;}
						}
						else if (strcmp(t->refID, "chrY")==0)
						{
						    arrow_builders_append(24, t->beginPos, p->str.s);countsY++;
						 }
						else if (strcmp(t->refID, "chrM")==0)
						{
						    arrow_builders_append(25, t->beginPos, p->str.s);countsM++;
						 }
					}
				} else if ((p->opt->flag & MM_F_PAF_NO_HIT) || ((p->opt->flag & MM_F_OUT_SAM) && !(p->opt->flag & MM_F_SAM_HIT_ONLY))) { // output an empty hit, if requested
					if (p->opt->flag & MM_F_OUT_SAM)
						mm_write_sam3(&p->str, mi, t, i - seg_st, -1, s->n_seg[k], &s->n_reg[seg_st], (const mm_reg1_t*const*)&s->reg[seg_st], km, p->opt->flag, s->rep_len[i]);
					else
						mm_write_paf3(&p->str, mi, t, 0, 0, p->opt->flag, s->rep_len[i]);
					//mm_err_puts(p->str.s);
					if (strcmp(t->refID, "chr1")==0) //t->beginPos
						{
						    if (t->beginPos < 49850124 )  
						    {arrow_builders_append(1, t->beginPos, p->str.s);counts1++;}
						    else if (t->beginPos >= 49850124 && t->beginPos < 99700248) 
						    {arrow_builders_append(110, t->beginPos, p->str.s);counts1_1++;}
						    else if (t->beginPos >= 99700248 && t->beginPos < 149550372) 
						    {arrow_builders_append(120, t->beginPos, p->str.s);counts1_2++;}
						    else if (t->beginPos >= 149550372 && t->beginPos < 199400496)
						    {arrow_builders_append(130, t->beginPos, p->str.s);counts1_3++;}
						    else {arrow_builders_append(140, t->beginPos, p->str.s);counts1_4++;}
						}
						else if (strcmp(t->refID, "chr2")==0)
						{
						    if (t->beginPos < 48438705 )  
						    {arrow_builders_append(2, t->beginPos, p->str.s);counts2++;}
						    else if (t->beginPos >= 48438705 && t->beginPos < 96877410) 
						    {arrow_builders_append(210, t->beginPos, p->str.s);counts2_1++;}
						    else if (t->beginPos >= 96877410 && t->beginPos < 145316115) 
						    {arrow_builders_append(220, t->beginPos, p->str.s);counts2_2++;}
						    else if (t->beginPos >= 145316115 && t->beginPos < 193754820)
						    {arrow_builders_append(230, t->beginPos, p->str.s);counts2_3++;}
						    else {arrow_builders_append(240, t->beginPos, p->str.s);counts2_4++;}
						}
						else if (strcmp(t->refID, "chr3")==0)
						{
						    if (t->beginPos < 49573889 )  
						    {arrow_builders_append(3, t->beginPos, p->str.s);counts3++;}
						    else if (t->beginPos >= 49573889 && t->beginPos < 99147778) 
						    {arrow_builders_append(31, t->beginPos, p->str.s);counts3_1++;}
						    else if (t->beginPos >= 99147778 && t->beginPos < 148721667) 
						    {arrow_builders_append(32, t->beginPos, p->str.s);counts3_2++;}
						    else {arrow_builders_append(33, t->beginPos, p->str.s);counts3_3++;}
						}
						else if (strcmp(t->refID, "chr4")==0)
						{
						    if (t->beginPos < 49573889 )  
						    {arrow_builders_append(4, t->beginPos, p->str.s);counts4++;}
						    else if (t->beginPos >= 49573889 && t->beginPos < 99147778) 
						    {arrow_builders_append(41, t->beginPos, p->str.s);counts4_1++;}
						    else if (t->beginPos >= 99147779 && t->beginPos < 148721667) 
						    {arrow_builders_append(42, t->beginPos, p->str.s);counts4_2++;}
						    else {arrow_builders_append(43, t->beginPos, p->str.s);counts4_3++;}
						}
						else if (strcmp(t->refID, "chr5")==0)
						{
						    if (t->beginPos < 49573889 )  
						    {arrow_builders_append(5, t->beginPos, p->str.s);counts5++;}
						    else if (t->beginPos >= 49573889 && t->beginPos < 99147778) 
						    {arrow_builders_append(51, t->beginPos, p->str.s);counts5_1++;}
						    else if (t->beginPos >= 99147778 && t->beginPos < 148721667) 
						    {arrow_builders_append(52, t->beginPos, p->str.s);counts5_2++;}
						    else {arrow_builders_append(53, t->beginPos, p->str.s);counts5_3++;}
						}
						else if (strcmp(t->refID, "chr6")==0)
						{
						    if (t->beginPos < 42701494 )  
						    {arrow_builders_append(6, t->beginPos, p->str.s);counts6++;}
						    else if (t->beginPos >= 42701494 && t->beginPos < 85402988) 
						    {arrow_builders_append(61, t->beginPos, p->str.s);counts6_1++;}
						    else if (t->beginPos >= 85402988 && t->beginPos < 128104482) 
						    {arrow_builders_append(62, t->beginPos, p->str.s);counts6_2++;}
						    else {arrow_builders_append(63, t->beginPos, p->str.s);counts6_3++;}
						}
						else if (strcmp(t->refID, "chr7")==0)
						{
						    if (t->beginPos < 53115324 )  
						    {arrow_builders_append(7, t->beginPos, p->str.s);counts7++;}
						    else if (t->beginPos >= 53115324 && t->beginPos < 106230648) 
						    {arrow_builders_append(71, t->beginPos, p->str.s);counts7_1++;}
						    else {arrow_builders_append(72, t->beginPos, p->str.s);counts7_2++;}
						}
						else if (strcmp(t->refID, "chr8")==0)
						{
						    if (t->beginPos < 48379545 )  
						    {arrow_builders_append(8, t->beginPos, p->str.s);counts8++;}
						    else if (t->beginPos >= 48379545 && t->beginPos < 96759090) 
						    {arrow_builders_append(81, t->beginPos, p->str.s);counts8_1++;}
						    else {arrow_builders_append(82, t->beginPos, p->str.s);counts8_2++;}
						}
						else if (strcmp(t->refID, "chr9")==0)
						{
						    if (t->beginPos < 46131572 )  
						    {arrow_builders_append(9, t->beginPos, p->str.s);counts9++;}
						    else if (t->beginPos >= 46131572 && t->beginPos < 92263144) 
						    {arrow_builders_append(91, t->beginPos, p->str.s);counts9_1++;}
						    else {arrow_builders_append(92, t->beginPos, p->str.s);counts9_2++;}
						}
						else if (strcmp(t->refID, "chr10")==0)
						{
						    if (t->beginPos < 44599140 )  
						    {arrow_builders_append(10, t->beginPos, p->str.s);counts10++;}
						    else if (t->beginPos >= 44599140 && t->beginPos < 89198280) 
						    {arrow_builders_append(101, t->beginPos, p->str.s);counts10_1++;}
						    else {arrow_builders_append(102, t->beginPos, p->str.s);counts10_2++;}
						}
						else if (strcmp(t->refID, "chr11")==0)
						{
						    if (t->beginPos < 45028874 )  
						    {arrow_builders_append(11, t->beginPos, p->str.s);counts11++;}
						    else if (t->beginPos >= 45028874 && t->beginPos < 90057748) 
						    {arrow_builders_append(111, t->beginPos, p->str.s);counts11_1++;}
						    else {arrow_builders_append(112, t->beginPos, p->str.s);counts11_2++;}
						}
						else if (strcmp(t->refID, "chr12")==0)
						{
						    if (t->beginPos < 44425103 )  
						    {arrow_builders_append(12, t->beginPos, p->str.s);counts12++;}
						    else if (t->beginPos >= 44425103 && t->beginPos < 88850206) 
						    {arrow_builders_append(121, t->beginPos, p->str.s);counts12_1++;}
						    else {arrow_builders_append(122, t->beginPos, p->str.s);counts12_2++;}
						}
						else if (strcmp(t->refID, "chr13")==0)
						{
						    if (t->beginPos < 57182164 )  
						    {arrow_builders_append(13, t->beginPos, p->str.s);counts13++;}
						    else {arrow_builders_append(131, t->beginPos, p->str.s);counts13_1++;}
						}
						else if (strcmp(t->refID, "chr14")==0)
						{
						    if (t->beginPos < 53180292)  
						    {arrow_builders_append(14, t->beginPos, p->str.s);counts14++;}
						    else {arrow_builders_append(141, t->beginPos, p->str.s);counts14_1++;}
						}
						else if (strcmp(t->refID, "chr15")==0)
						{
						    if (t->beginPos < 50995594 )  
						    {arrow_builders_append(15, t->beginPos, p->str.s);counts15++;}
						    else {arrow_builders_append(151, t->beginPos, p->str.s);counts15_1++;}
						}
						else if (strcmp(t->refID, "chr16")==0)
						{
						    if (t->beginPos < 45169172 )  
						    {arrow_builders_append(16, t->beginPos, p->str.s);counts16++;}
						    else {arrow_builders_append(161, t->beginPos, p->str.s);counts16_1++;}
						}
						else if (strcmp(t->refID, "chr17")==0)
						{
						    if (t->beginPos < 41628720 )  
						    {arrow_builders_append(17, t->beginPos, p->str.s);counts17++;}
						    else {arrow_builders_append(171, t->beginPos, p->str.s);counts17_1++;}
						}
						else if (strcmp(t->refID, "chr18")==0)
						{
						    if (t->beginPos < 40186642 )  
						    {arrow_builders_append(18, t->beginPos, p->str.s);counts18++;}
						    else {arrow_builders_append(181, t->beginPos, p->str.s);counts18_1++;}
						}
						else if (strcmp(t->refID, "chr19")==0)
						{
						    arrow_builders_append(19, t->beginPos, p->str.s);counts19++;
						 }
						else if (strcmp(t->refID, "chr20")==0)
						{
						    arrow_builders_append(20, t->beginPos, p->str.s);counts20++;
						 }
						else if (strcmp(t->refID, "chr21")==0)
						{
						    arrow_builders_append(21, t->beginPos, p->str.s);counts21++;
						 }
						else if (strcmp(t->refID, "chr22")==0)
						{
						    arrow_builders_append(22, t->beginPos, p->str.s);counts22++;
						 }
						else if (strcmp(t->refID, "chrX")==0)
						{
						    if (t->beginPos < 52013631 )  
						    {arrow_builders_append(23, t->beginPos, p->str.s);countsX++;}
						    else if (t->beginPos >= 52013631 && t->beginPos < 104027262) 
						    {arrow_builders_append(231, t->beginPos, p->str.s);countsX_1++;}
						    else {arrow_builders_append(232, t->beginPos, p->str.s);countsX_2++;}
						}
						else if (strcmp(t->refID, "chrY")==0)
						{
						    arrow_builders_append(24, t->beginPos, p->str.s);countsY++;
						 }
						else if (strcmp(t->refID, "chrM")==0)
						{
						    arrow_builders_append(25, t->beginPos, p->str.s);countsM++;
						 }
				}
			}
			for (i = seg_st; i < seg_en; ++i) {
				for (j = 0; j < s->n_reg[i]; ++j) free(s->reg[i][j].p);
				free(s->reg[i]);
				free(s->seq[i].seq); free(s->seq[i].name);
				if (s->seq[i].qual) free(s->seq[i].qual);
				if (s->seq[i].comment) free(s->seq[i].comment);
			}
		}
		free(s->reg); free(s->n_reg); free(s->seq); // seg_off, n_seg, rep_len and frag_gap were allocated with reg; no memory leak here
		km_destroy(km);
		if (mm_verbose >= 3)
			fprintf(stderr, "[M::%s::%.3f*%.2f] mapped %d sequences\n", __func__, realtime() - mm_realtime0, cputime() / (realtime() - mm_realtime0), s->n_seq);
		free(s);
	}
    return 0;
}

static mm_bseq_file_t **open_bseqs(int n, const char **fn)
{
	mm_bseq_file_t **fp;
	int i, j;
	fp = (mm_bseq_file_t**)calloc(n, sizeof(mm_bseq_file_t*));
	for (i = 0; i < n; ++i) {
		if ((fp[i] = mm_bseq_open(fn[i])) == 0) {
			if (mm_verbose >= 1)
				fprintf(stderr, "ERROR: failed to open file '%s': %s\n", fn[i], strerror(errno));
			for (j = 0; j < i; ++j)
				mm_bseq_close(fp[j]);
			free(fp);
			return 0;
		}
	}
	return fp;
}

int mm_map_file_frag(const mm_idx_t *idx, int n_segs, const char **fn, const mm_mapopt_t *opt, int n_threads)
{
	int i, pl_threads;
	pipeline_t pl;
	if (n_segs < 1) return -1;
	memset(&pl, 0, sizeof(pipeline_t));
	pl.n_fp = n_segs;
	pl.fp = open_bseqs(pl.n_fp, fn);
	if (pl.fp == 0) return -1;
	pl.opt = opt, pl.mi = idx;
	pl.n_threads = n_threads > 1? n_threads : 1;
	pl.mini_batch_size = opt->mini_batch_size;
	if (opt->split_prefix)
		pl.fp_split = mm_split_init(opt->split_prefix, idx);
	pl_threads = n_threads == 1? 1 : (opt->flag&MM_F_2_IO_THREADS)? 3 : 2;
	kt_pipeline(pl_threads, worker_pipeline, &pl, 3);

	free(pl.str.s);
	if (pl.fp_split) fclose(pl.fp_split);
	for (i = 0; i < pl.n_fp; ++i)
		mm_bseq_close(pl.fp[i]);
	free(pl.fp);
	return 0;
}

int mm_map_file(const mm_idx_t *idx, const char *fn, const mm_mapopt_t *opt, int n_threads)
{
	return mm_map_file_frag(idx, 1, &fn, opt, n_threads);
}

int mm_split_merge(int n_segs, const char **fn, const mm_mapopt_t *opt, int n_split_idx)
{
	int i;
	pipeline_t pl;
	mm_idx_t *mi;
	if (n_segs < 1 || n_split_idx < 1) return -1;
	memset(&pl, 0, sizeof(pipeline_t));
	pl.n_fp = n_segs;
	pl.fp = open_bseqs(pl.n_fp, fn);
	if (pl.fp == 0) return -1;
	pl.opt = opt;
	pl.mini_batch_size = opt->mini_batch_size;

	pl.n_parts = n_split_idx;
	pl.fp_parts  = CALLOC(FILE*, pl.n_parts);
	pl.rid_shift = CALLOC(uint32_t, pl.n_parts);
	pl.mi = mi = mm_split_merge_prep(opt->split_prefix, n_split_idx, pl.fp_parts, pl.rid_shift);
	if (pl.mi == 0) {
		free(pl.fp_parts);
		free(pl.rid_shift);
		return -1;
	}
	for (i = n_split_idx - 1; i > 0; --i)
		pl.rid_shift[i] = pl.rid_shift[i - 1];
	for (pl.rid_shift[0] = 0, i = 1; i < n_split_idx; ++i)
		pl.rid_shift[i] += pl.rid_shift[i - 1];
	if (opt->flag & MM_F_OUT_SAM)
		for (i = 0; i < (int32_t)pl.mi->n_seq; ++i)
			printf("@SQ\tSN:%s\tLN:%d\n", pl.mi->seq[i].name, pl.mi->seq[i].len);

	kt_pipeline(2, worker_pipeline, &pl, 3);

	free(pl.str.s);
	mm_idx_destroy(mi);
	free(pl.rid_shift);
	for (i = 0; i < n_split_idx; ++i)
		fclose(pl.fp_parts[i]);
	free(pl.fp_parts);
	for (i = 0; i < pl.n_fp; ++i)
		mm_bseq_close(pl.fp[i]);
	free(pl.fp);
	mm_split_rm_tmp(opt->split_prefix, n_split_idx);
	return 0;
}
