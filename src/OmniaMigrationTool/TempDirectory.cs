using System;
using System.IO;

namespace OmniaMigrationTool
{
    public sealed class TempDirectory : IDisposable
    {
        private string _path;
        public TempDirectory()
            : this(System.IO.Path.GetTempFileName())
        {
            CreateDirectory();
        }

        private TempDirectory(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentNullException(path);
            }
            _path = path;
        }

        public string Path
        {
            get
            {
                if (_path == null)
                {
                    throw new ObjectDisposedException($"{GetType().Name} was disposed");
                }
                return _path;
            }
        }

        private void CreateDirectory()
        {
            File.Delete(_path);
            Directory.CreateDirectory(_path);
        }

        ~TempDirectory()
            => Dispose(false);

        public void Dispose()
            => Dispose(true);

        private void Dispose(bool disposing)
        {
            if (disposing)
                GC.SuppressFinalize(this);

            if (_path != null)
            {
                try
                {
                    Directory.Delete(_path, true);
                }
                catch { }

                _path = null;
            }
        }
    }
}