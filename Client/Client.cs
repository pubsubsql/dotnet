/* Copyright (C) 2014 CompleteDB LLC.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with PubSubSQL.  If not, see <http://www.gnu.org/licenses/>.
 */

using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.Serialization.Json;
using System.Runtime.Serialization;
using System.IO;
using System.Net.Sockets;

namespace PubSubSQL
{
    [DataContract]
    class responseData
    {
        [DataMember(Name = "status")]
        public string Status { get; set; }
        [DataMember(Name = "msg")]
        public string Msg { get; set; }
        [DataMember(Name = "action")]
        public string Action { get; set; }
        [DataMember(Name = "pubsubid")]
        public string PubSubId { get; set; }
        [DataMember(Name = "rows")]
        public int Rows { get; set; }
        [DataMember(Name = "fromrow")]
        public int Fromrow { get; set; }
        [DataMember(Name = "torow")]
        public int Torow { get; set; }
        [DataMember(Name = "columns")]
        public List<string> Columns { get; set; }
        [DataMember(Name = "data")]
        public List<List<string>> Values { get; set; }
    }

    public class  Client
    {
        string host;
        int port;
        NetHelper rw = new NetHelper();
        UInt32 requestId;
        string err;
        byte[] rawjson;
        responseData response = new responseData();
        Dictionary<string, int> columns = new Dictionary<string, int>(10);
        int record;
        Queue<byte[]> backlog = new Queue<byte[]>();
        const int CLIENT_DEFAULT_BUFFER_SIZE = 2048;

        public Client()
        {
            reset();
        }

        /// <summary>
        /// Connect connects the Client to the pubsubsql server.
        /// Address string has the form host:port.
        /// </summary>
        public void Connect(string address)
        {
            Disconnect();
            // validate address
            int sep = address.IndexOf(':');
            if (sep < 0)
            {
                throw new ArgumentException("Invalid network address");
            }
            // set host and port
            host = address.Substring(0, sep);
            toPort(ref port, address.Substring(sep + 1));
            // connect
            Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(host, port);
            rw.Set(socket, CLIENT_DEFAULT_BUFFER_SIZE); 
        }

        /// <summary>
        /// Disconnect disconnects the Client from the pubsubsql server.
        /// </summary>
        public void Disconnect()
        {
            backlog.Clear();
            try
            {
                if (Connected)
                {
                    write("close");
                }
            }
            catch (Exception e)
            {

            }
            reset();
            rw.Close();
        }

        /// <summary>
        /// Connected returns true if the Client is currently connected to the pubsubsql server.
        /// </summary>
        public bool Connected
        {
            get { return rw.Valid(); }
        }

        /// <summary>
        /// Execute executes a command against the pubsubsql server.
        /// The pubsubsql server returns to the Client a response in JSON format.
        /// </summary>
        public void Execute(string command)
        {
            reset();
            write(command);
            NetHeader header = new NetHeader();
            for (;;)
            {
                byte[] bytes = null;
                reset();
                read(ref header, out bytes);
                if (header.RequestId == requestId)
                {
                    // response we are waiting for
                    unmarshalJSON(bytes);
                    return;
                }
                else if (header.RequestId == 0)
                {
                    // pubsub action, save it and skip it for now
                    // will be proccesed next time WaitPubSub is called
                    backlog.Enqueue(bytes);
                }
                else if (header.RequestId < this.requestId)
                {
                    // we did not read full result set from previous command ignore it or flag and error?
                    // for now lets ignore it, continue reading until we hit our request id 
                    reset();
                }
                else
                {
                    // this should never happen
                    throw new Exception("Protocol error invalid requestId");
                }
            }
        }

        /// <summary>
        /// Stream sends a command to the pubsubsql server without returning a response.
        /// </summary>
        public void Stream(string command)
        {
            reset();
            //TODO optimize
            write("stream " + command);
        }

        /// <summary>
        /// JSON returns a response string in JSON format from the 
        /// last command executed against the pubsubsql server.
        /// </summary>
        public string JSON
        {
            get
            {
                if (rawjson == null) return string.Empty;
                return System.Text.UTF8Encoding.UTF8.GetString(rawjson);
            }
        }


        /// <summary>
        /// Action returns an action string from the response 
        /// returned by the last command executed against the pubsubsql server.
        /// </summary>
        public string Action
        {
            get
            {
                if (response.Action == null) return string.Empty;
                return response.Action;
            }
        }

        /// <summary>
        /// PubSubId returns a unique identifier generated by the pubsubsql server when 
        /// a Client subscribes to a table. If the client has subscribed to more than  one table, 
        /// PubSubId should be used by the Client to uniquely identify messages 
        /// published by the pubsubsql server.
        /// </summary>
        public string PubSubId
        {
            get
            {
                if (response.PubSubId == null) return string.Empty;
                return response.PubSubId;
            }
        }

        /// <summary>
        /// RowCount returns the number of rows in the result set returned by the pubsubsql server.
        /// </summary>
        public int RowCount
        {
            get { return response.Rows; } 
        }

        /// <summary>
        /// NextRow is used to move to the next row in the result set returned by the pubsubsql server.    
        /// When called for the first time, NextRow moves to the first row in the result set.
        /// Returns false when all rows are read.
        /// </summary>
        public bool NextRow()
        {
            for (;;)
            {
                // no resulst set
                if (response.Rows == 0) return false;
                if (response.Fromrow == 0 || response.Torow == 0) return false;
                // the current record is valid
                record++;
                if (record <= (response.Torow - response.Fromrow)) return true;
                // we reached the end of the result set
                if (response.Rows == response.Torow)
                {
                    record--;
                    return false;
                }
                // if we are here there is another batch
                reset();
                NetHeader header = new NetHeader();
                byte[] bytes = null;
                read(ref header, out bytes);
                if (header.RequestId > 0 && header.RequestId != this.requestId)
                {
                    protocolError();
                    return false;
                }
                unmarshalJSON(bytes);
            }
        }

        /// <summary>
        /// GetValue returns the value within the current row for the given column name.
        /// If the column name does not exist, GetValue returns an empty string.	
        /// </summary>
        public string GetValue(string column)
        {
            int ordinal = -1;
            if (record < 0 || record >= response.Values.Count) return string.Empty;
            if (response.Values == null || !columns.TryGetValue(column, out ordinal)) return string.Empty;
            return response.Values[record][ordinal];
        }

        /// <summary>
        /// GetValue returns the value within the current row for the given column ordinal.
        /// The column ordinal represents the zero based position of the column in the Columns collection of the result set.
        /// If the column ordinal is out of range, GetValue returns an empty string.		
        /// </summary>
        public string GetValue(int ordinal)
        {
            if (ordinal < 0) return string.Empty;
            if (record < 0 || record >= response.Values.Count) return string.Empty;
            if (response.Values == null || response.Columns.Count <= ordinal) return string.Empty;
            return response.Values[record][ordinal];
        }

        /// <summary>
        /// HasColumn determines if the column name exists in the columns collection of the result set.
        /// </summary>
        public bool HasColumn(string column)
        {
            return columns.ContainsKey(column);
        }

        /// <summary>
        /// ColumnCount returns the number of columns in the columns collection of the result set.
        /// </summary>
        public int ColumnCount
        {
            get
            {
                if (response.Columns == null) return 0;
                return response.Columns.Count;
            }
        }

        /// <summary>
        /// Columns returns the column names in the columns collection of the result set.
        /// </summary>
        public IEnumerable<string> Columns
        {
            get
            {
                if (response.Columns == null)
                {
                    return new List<string>();
                }
                return response.Columns;
            }
        }

        /// <summary>
        /// WaitForPubSub waits until the pubsubsql server publishes a message for
        /// the subscribed Client or until the timeout interval elapses.
        /// Returns false when timeout interval elapses.
        /// </summary>
        public bool WaitForPubSub(int timeout)
        {
            // timed out
            if (timeout <= 0)
            {
                return false;
            }
            // process backlog first
            reset();
            if (backlog.Count > 0)
            {
                byte[] bytes = backlog.Dequeue();
                unmarshalJSON(bytes);
                return true;
            }
            for (;;)
            {
                byte[] bytes = null;
                NetHeader header = new NetHeader();
                // return on error
                if (!readTimeout(timeout, ref header, out bytes)) return false;
                // we got what we were looking for
                if (header.RequestId == 0)
                {
                    unmarshalJSON(bytes);
                    return true; 
                }
                // this is not pubsub message; are we reading abandoned result set 
                // ignore and continue reading do we want to adjust time out value here?
                // TODO?
            }
        }

        private void reset()
        {
            err = string.Empty;
            response = new responseData();
            columns.Clear();
            rawjson = null;
            this.record = -1;
        }

        private void toPort(ref int port, string sport)
        {
            try
            {
                port = Convert.ToInt32(sport, 10);
            }
            catch (Exception )
            {
                throw new ArgumentException("Invalid port " + sport);
            }
        }

        private void protocolError()
        {
            Disconnect();
            throw new Exception("Protocol error");
        }

        private void write(string message)
        {
            try
            {
                if (!rw.Valid()) throw new Exception("Not connected");
                requestId++;
                rw.WriteWithHeader(requestId, NetHelper.ToUTF8(message));
            }
            catch (Exception e)
            {
                hardDisconnect();
                throw e;
            }
        }

        private bool readTimeout(int timeout, ref NetHeader header, out byte[] bytes)
        {
            bytes = null;
            try
            {
                if (!rw.Valid()) throw new InvalidOperationException("Not connected");
                return rw.ReadTimeout(timeout, ref header, out bytes);
            }
            catch (Exception e)
            {
                hardDisconnect();
                throw e;
            }
        }

        private void hardDisconnect()
        {
            backlog.Clear();
            rw.Close();
            reset();
        }

        private void read(ref NetHeader header, out byte[] bytes)
        {
            const int MAX_READ_TIMEOUT_MILLISECONDS = 1000 * 60 * 3;
            bool timedout = readTimeout(MAX_READ_TIMEOUT_MILLISECONDS, ref header, out bytes);
            if (!timedout)
            {
                throw new TimeoutException();
            }
        }

        private void unmarshalJSON(byte[] bytes)
        {
            rawjson = bytes;
            MemoryStream stream = new MemoryStream(bytes);
            DataContractJsonSerializer jsonSerializer = new DataContractJsonSerializer(typeof(responseData));
            response = jsonSerializer.ReadObject(stream) as responseData;
            if (response != null && response.Status != "ok")
            {
                throw new ArgumentException(response.Msg);
            }
            setColumns();
        }

        private void setColumns()
        {
            if (response.Columns != null)
            {
                int index = 0; 
                foreach (string column in response.Columns) 
                {
                    columns[column] = index;
                    index++;
                }
            }
        }

    }

}
