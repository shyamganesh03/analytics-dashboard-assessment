import * as xlsx from 'xlsx'
import { db } from '@vercel/postgres'

function convertPointData(pointData: string) {
  const cleanedData = pointData.replace('POINT (', '').replace(')', '')
  return cleanedData?.trim()
}

export async function POST(request: Request) {
  const formData: any = await request.formData()
  const file = formData.get('file')

  // Convert Blob to Buffer
  const buffer = Buffer.from(await file.arrayBuffer())
  const workbook = xlsx.read(buffer, { type: 'buffer' })
  const sheetName = workbook.SheetNames[0]
  const worksheet = workbook.Sheets[sheetName]
  const jsonData = xlsx.utils.sheet_to_json(worksheet)

  const finalJsonData = jsonData.map((item: any) => ({
    vin: item['VIN (1-10)'],
    county: item['County'],
    city: item['City'],
    state: item['State'],
    postal_code: item['Postal Code'],
    model_year: item['Model Year'],
    make: item['Make'],
    model: item['Model'],
    electric_vehicle_type: item['Electric Vehicle Type'],
    cafv_eligibility: item['Clean Alternative Fuel Vehicle (CAFV) Eligibility'],
    electric_range: item['Electric Range'],
    base_msrp: item['Base MSRP'],
    legislative_district: item['Legislative District'],
    dol_vehicle_id: item['DOL Vehicle ID'],
    vehicle_location: convertPointData(item['Vehicle Location'] || ''),
    electric_utility: item['Electric Utility'],
    census_tract: item['2020 Census Tract'],
  }))

  const startIndex = formData?.get('startIndex') || 0
  const endIndex = formData?.get('endIndex') || finalJsonData?.length
  let insertedCount = 0
  let ql
  const postgresClient = await db.connect()
  try {
    const batchData = finalJsonData.slice(startIndex, endIndex)
    const values = batchData.map((row) => [
      `'${row.vin}'`,
      `'${row.county}'`,
      `'${row.city}'`,
      `'${row.state}'`,
      `'${row.postal_code}'`,
      `'${row.model_year}'`,
      `'${row.make}'`,
      `'${row.model}'`,
      `'${row.electric_vehicle_type}'`,
      `'${row.cafv_eligibility}'`,
      `'${row.electric_range}'`,
      `'${row.base_msrp}'`,
      `'${row.legislative_district}'`,
      `'${row.dol_vehicle_id}'`,
      `'${row.vehicle_location}'`,
      `'${row.electric_utility}'`,
      `'${row.census_tract}'`,
    ])

    const query = `
        INSERT INTO electric_vehicle_population (
          vin, county, city, state, postal_code, model_year, make, model, 
          electric_vehicle_type, cafv_eligibility, electric_range, base_msrp, 
          legislative_district, dol_vehicle_id, vehicle_location, electric_utility, census_tract
        ) VALUES ${values.map((_, idx) => `(${values[idx]?.join(', ')})`).join(', ')}
      `
    ql = query
    // await postgresClient.query(query)
    insertedCount = values?.length
  } catch (error) {
    postgresClient.release()
    return Response.json(
      {
        response: 'Failed to insert',
        status: 500,
        error,
      },
      {
        status: 500,
      },
    )
  }
  postgresClient.release()
  return Response.json({
    response: 'Successfully inserted',
    total: finalJsonData?.length,
    insertedCount,
  })
}
