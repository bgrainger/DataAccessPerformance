// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Direct;

namespace BenchmarkDb
{
    public sealed class MySqlDirectDriver : DriverBase, IDisposable
    {
	    string _connectionString;

        public override Func<Task> TryGetVariation(string variationName)
        {
            switch (variationName)
            {
                case Variation.AsyncCaching:
                    return DoWorkAsyncCaching;
                default:
                    return NotSupportedVariation;
            }
        }

        public override void Initialize(string connectionString, int threadCount)
        {
	        _connectionString = connectionString;
        }
		
        public override async Task DoWorkAsyncCaching()
        {
	        var csb = new MySqlConnectionStringBuilder(_connectionString);
	        var session = new MySqlSession(csb.Server, (int) csb.Port, csb.UserID, csb.Password, csb.Database);

			await session.ConnectAsync();

            while (Program.IsRunning)
            {
                var results = new List<Fortune>();

                await session.ExecuteAsync(Program.TestQuery);

	            while (await session.ReadAsync())
	            {
					results.Add(new Fortune() { Id = session.ReadInt32(), Message = session.ReadString() });
	            }

                Program.IncrementCounter();
            }
        }

        public void Dispose()
        {
        }
    }
}
