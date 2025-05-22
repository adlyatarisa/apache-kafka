# Apache Kafka Project: Real-Time Monitoring

Adlya Isriena Aftarisya - 5027231066

## Penjelasan Code
1. **Producer Kafka**  
   - Terdapat dua producer: satu untuk mengirim data suhu dan satu untuk kelembaban.  
   - Data dikirim dalam format JSON setiap detik dengan informasi `gudang_id`, `suhu`, atau `kelembaban`.

2. **Consumer PySpark**  
   - Mengonsumsi data dari topik Kafka (`sensor-suhu-gudang` dan `sensor-kelembaban-gudang`).
   - Melakukan filtering untuk mendeteksi suhu > 80°C atau kelembaban > 70%.
   - Menggabungkan stream dari kedua sensor berdasarkan `gudang_id` dan window waktu untuk mendeteksi kondisi bahaya ganda.

3. **Peringatan**  
   - Menampilkan peringatan suhu tinggi, kelembaban tinggi, atau peringatan kritis jika kedua kondisi terjadi bersamaan.

## Cara Menjalankan
1. **Setup Kafka**  
   - Jalankan Apache Kafka dan buat dua topik: `sensor-suhu-gudang` dan `sensor-kelembaban-gudang`.

2. **Jalankan Producer**  
   - Jalankan script producer untuk mengirim data suhu dan kelembaban ke topik Kafka.

3. **Jalankan PySpark Consumer**  
   - Jalankan script PySpark untuk mengonsumsi data dari Kafka, melakukan filtering, dan menampilkan peringatan.

4. **Gabungkan Stream**  
   - Pastikan script PySpark juga melakukan join stream untuk mendeteksi kondisi bahaya ganda.

## Output
![alt text](<dokumentasi.png>)