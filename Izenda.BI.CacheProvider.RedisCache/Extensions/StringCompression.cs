using System;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace Izenda.BI.CacheProvider.RedisCache.Extensions
{
    public static class StringCompression
    {
        /// <summary>
        /// Compressess the string
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string Compress(this string value)
        {
            byte[] stringBytes = Encoding.UTF8.GetBytes(value);
            string newValue = "";

            using (var outputStream = new MemoryStream())
            {
                using (var gzipStream = new GZipStream(outputStream, CompressionMode.Compress))
                {
                    gzipStream.Write(stringBytes, 0, stringBytes.Length);
                }

                var outputBytes = outputStream.ToArray();
                newValue = Convert.ToBase64String(outputBytes);
            }

            return newValue;
        }

        /// <summary>
        /// Decompresses the string
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string Decompress(this string value)
        {
            byte[] inputBytes = Convert.FromBase64String(value);
            string newValue = "";

            using (var inputStream = new MemoryStream(inputBytes))
            using (var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress))
            using (var streamReader = new StreamReader(gzipStream))
            {
                newValue = streamReader.ReadToEnd();
            }

            return newValue;
        }

        /// <summary>
        /// Returns a StreamReader from the compressed string
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static StreamReader DecompressToStreamReader(this string value)
        {
            byte[] inputBytes = Convert.FromBase64String(value);

            MemoryStream inputStream = new MemoryStream(inputBytes);
            GZipStream gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);
            StreamReader streamReader = new StreamReader(gzipStream);

            return streamReader;
        }
    }
}
