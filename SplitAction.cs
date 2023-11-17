using System.Buffers;
using TiffLibrary;

namespace SplitBigTiffApp
{
    internal class SplitAction
    {
        protected SplitAction()
        {
        }

        public static async Task<int> Split(string inputFile, CancellationToken cancellationToken)
        {


            TiffStreamOffset ifdOffset;

            await using TiffFileReader reader = await TiffFileReader.OpenAsync(inputFile, cancellationToken);
            await using TiffFileContentReader contentReader = await reader.CreateContentReaderAsync(cancellationToken);
            await using TiffFieldReader fieldReader = await reader.CreateFieldReaderAsync(cancellationToken);

            TiffStreamOffset inputIfdOffset = reader.FirstImageFileDirectoryOffset;

            var counter = 1;

            while (!inputIfdOffset.IsZero)
            {
                TiffImageFileDirectory ifd = await reader.ReadImageFileDirectoryAsync(inputIfdOffset, cancellationToken);

                var outputFileName = $"{Path.GetFileNameWithoutExtension(inputFile)}_{counter}.tiff";

                Console.WriteLine($"Creating: {outputFileName}");

                await using TiffFileWriter writer = await TiffFileWriter.OpenAsync(outputFileName, cancellationToken: cancellationToken);
                using TiffImageFileDirectoryWriter ifdWriter = writer.CreateImageFileDirectory();

                await CopyIfdAsync(contentReader, fieldReader, ifd, ifdWriter, cancellationToken);
                ifdOffset = await ifdWriter.FlushAsync(default, cancellationToken);

                writer.SetFirstImageFileDirectoryOffset(ifdOffset);
                await writer.FlushAsync(cancellationToken);
                inputIfdOffset = ifd.NextOffset;
                counter++;
            }

            return 0;
        }

        private static async Task CopyIfdAsync(TiffFileContentReader contentReader, TiffFieldReader fieldReader, TiffImageFileDirectory ifd, TiffImageFileDirectoryWriter dest, CancellationToken cancellationToken)
        {
            var tagReader = new TiffTagReader(fieldReader, ifd);
            foreach (TiffImageFileDirectoryEntry entry in ifd)
            {
                // Stripped image data
                if (entry.Tag == TiffTag.StripOffsets)
                {
                    await CopyStrippedImageAsync(contentReader, tagReader, dest, cancellationToken);
                }
                else if (entry.Tag == TiffTag.StripByteCounts)
                {
                    // Ignore this
                }

                // Tiled image data
                else if (entry.Tag == TiffTag.TileOffsets)
                {
                    await CopyTiledImageAsync(contentReader, tagReader, dest, cancellationToken);
                }
                else if (entry.Tag == TiffTag.TileByteCounts)
                {
                    // Ignore this
                }

                // Other fields
                else
                {
                    await CopyFieldValueAsync(fieldReader, entry, dest, cancellationToken);
                }
            }
        }

        private static async Task CopyStrippedImageAsync(TiffFileContentReader contentReader, TiffTagReader tagReader, TiffImageFileDirectoryWriter dest, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            TiffValueCollection<ulong> offsets = await tagReader.ReadStripOffsetsAsync(cancellationToken);
            TiffValueCollection<ulong> byteCounts = await tagReader.ReadStripByteCountsAsync(cancellationToken);

            if (offsets.Count != byteCounts.Count)
            {
                throw new InvalidDataException("Failed to copy stripped image data. StripOffsets and StripByteCounts don't have the same amount of elements.");
            }

            uint[] offsets32 = new uint[offsets.Count];
            uint[] byteCounts32 = new uint[offsets.Count];

            byte[]? buffer = null;
            try
            {
                for (int i = 0; i < offsets.Count; i++)
                {
                    long offset = checked((long)offsets[i]);
                    int byteCount = checked((int)byteCounts[i]);
                    byteCounts32[i] = checked((uint)byteCount);

                    if (buffer is null || byteCount > buffer.Length)
                    {
                        if (buffer is not null)
                        {
                            ArrayPool<byte>.Shared.Return(buffer);
                        }
                        buffer = ArrayPool<byte>.Shared.Rent(byteCount);
                    }

                    if (await contentReader.ReadAsync(offset, new ArraySegment<byte>(buffer, 0, byteCount), cancellationToken) != byteCount)
                    {
                        throw new InvalidDataException("Invalid ByteCount field.");
                    }

                    TiffStreamOffset region = await dest.FileWriter.WriteAlignedBytesAsync(buffer, 0, byteCount, cancellationToken);
                    offsets32[i] = checked((uint)region.Offset);
                }
            }
            finally
            {
                if (buffer is not null)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }

            await dest.WriteTagAsync(TiffTag.StripOffsets, TiffValueCollection.UnsafeWrap(offsets32), cancellationToken);
            await dest.WriteTagAsync(TiffTag.StripByteCounts, TiffValueCollection.UnsafeWrap(byteCounts32), cancellationToken);
        }

        private static async Task CopyTiledImageAsync(TiffFileContentReader contentReader, TiffTagReader tagReader, TiffImageFileDirectoryWriter dest, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            TiffValueCollection<ulong> offsets = await tagReader.ReadTileOffsetsAsync(cancellationToken);
            TiffValueCollection<ulong> byteCounts = await tagReader.ReadTileByteCountsAsync(cancellationToken);

            if (offsets.Count != byteCounts.Count)
            {
                throw new InvalidDataException("Failed to copy stripped image data. TileOffsets and TileByteCounts don't have the same amount of elements.");
            }

            uint[] offsets32 = new uint[offsets.Count];
            uint[] byteCounts32 = new uint[offsets.Count];

            byte[]? buffer = null;
            try
            {
                for (int i = 0; i < offsets.Count; i++)
                {
                    long offset = checked((long)offsets[i]);
                    int byteCount = checked((int)byteCounts[i]);
                    byteCounts32[i] = checked((uint)byteCount);

                    if (buffer is null || byteCount > buffer.Length)
                    {
                        if (!(buffer is null))
                        {
                            ArrayPool<byte>.Shared.Return(buffer);
                        }
                        buffer = ArrayPool<byte>.Shared.Rent(byteCount);
                    }

                    if (await contentReader.ReadAsync(offset, new ArraySegment<byte>(buffer, 0, byteCount), cancellationToken) != byteCount)
                    {
                        throw new InvalidDataException("Invalid ByteCount field.");
                    }

                    TiffStreamOffset region = await dest.FileWriter.WriteAlignedBytesAsync(buffer, 0, byteCount, cancellationToken);
                    offsets32[i] = checked((uint)region.Offset);
                }
            }
            finally
            {
                if (!(buffer is null))
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }

            await dest.WriteTagAsync(TiffTag.TileOffsets, TiffValueCollection.UnsafeWrap(offsets32), cancellationToken);
            await dest.WriteTagAsync(TiffTag.TileByteCounts, TiffValueCollection.UnsafeWrap(byteCounts32), cancellationToken);
        }

        private static async Task CopyFieldValueAsync(TiffFieldReader reader, TiffImageFileDirectoryEntry entry, TiffImageFileDirectoryWriter ifdWriter, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            switch (entry.Type)
            {
                case TiffFieldType.Byte:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadByteFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.ASCII:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadASCIIFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.Short:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadShortFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.Long:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadLongFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.Rational:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadRationalFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.SByte:
                    await ifdWriter.WriteTagAsync(entry.Tag, entry.Type, await reader.ReadByteFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.Undefined:
                    await ifdWriter.WriteTagAsync(entry.Tag, entry.Type, await reader.ReadByteFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.SShort:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadSShortFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.SLong:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadSLongFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.SRational:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadSRationalFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.Float:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadFloatFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.Double:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadDoubleFieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.IFD:
                    throw new NotSupportedException("This IFD type is not supported yet.");
                case TiffFieldType.Long8:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadLong8FieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.SLong8:
                    await ifdWriter.WriteTagAsync(entry.Tag, await reader.ReadSLong8FieldAsync(entry, cancellationToken: cancellationToken), cancellationToken);
                    break;
                case TiffFieldType.IFD8:
                    throw new NotSupportedException("This IFD type is not supported yet.");
                default:
                    throw new NotSupportedException($"Unsupported IFD field type: {entry.Type}.");
            }

        }
    }
}
