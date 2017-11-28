﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Peregrine.Ado
{
    public class PeregrineDataReader : DbDataReader
    {
        private static readonly Task<bool> _trueResult = Task.FromResult(true);
        private static readonly Task<bool> _falseResult = Task.FromResult(false);
        
        private readonly ReadBuffer _readBuffer;

        public PeregrineDataReader(ReadBuffer readBuffer) 
            => _readBuffer = readBuffer;

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            var message = _readBuffer.ReadMessage();

            switch (message.Type)
            {
                case MessageType.DataRow:
                {
                    // Column count
                    _readBuffer.ReadShort();

                    return _trueResult;
                }

                case MessageType.CommandComplete:
                    return _falseResult;

                case MessageType.ErrorResponse:
                    throw new InvalidOperationException(_readBuffer.ReadErrorMessage());

                default:
                    throw new NotImplementedException(message.Type.ToString());
            }
        }

        // TODO: ordinal is ignored

        public override int GetInt32(int ordinal)
        {
            _readBuffer.ReadInt();

            return _readBuffer.ReadInt();
        }

        public override string GetString(int ordinal)
        {
            var length = _readBuffer.ReadInt();

            return _readBuffer.ReadString(length);
        }

        public override bool GetBoolean(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override byte GetByte(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override decimal GetDecimal(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override double GetDouble(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override float GetFloat(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override short GetInt16(int ordinal)
        {
            throw new NotImplementedException();
        }
        
        public override long GetInt64(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override string GetName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public override object GetValue(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public override bool IsDBNull(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int FieldCount => throw new NotImplementedException();

        public override object this[int ordinal] => throw new NotImplementedException();

        public override object this[string name] => throw new NotImplementedException();

        public override int RecordsAffected => throw new NotImplementedException();

        public override bool HasRows => throw new NotImplementedException();

        public override bool IsClosed => throw new NotImplementedException();

        public override bool NextResult()
        {
            throw new NotImplementedException();
        }

        public override bool Read()
        {
            throw new NotImplementedException();
        }

        public override int Depth => throw new NotImplementedException();

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}