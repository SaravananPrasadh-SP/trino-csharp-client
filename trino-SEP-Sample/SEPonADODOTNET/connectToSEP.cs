// See https://aka.ms/new-console-template for more information
using Trino.Client.Auth;
using Trino.Data.ADO.Server;
using Trino.Client;
using System.Data;
namespace SEPonADODOTNET
{
    class connectToSEP
    {
        static void Main(string[] args)
        {
            // defaults
            string defaultUrl = "http://localhost:8080";
            string defaultUser = "admin";
            string defaultPassword = "admin";
            string defaultRole = "public";
            // get the URL
            Console.Write("Enter URL: ");
            Console.Write($"Enter URL (default: {defaultUrl}): ");
            string urlInput = Console.ReadLine() ?? defaultUrl;
            string sepUrl = string.IsNullOrEmpty(urlInput) ? defaultUrl : urlInput;
            // get the user
            Console.Write($"Enter User (default: {defaultUser}): ");
            string userInput = Console.ReadLine() ?? defaultUser;
            string user = string.IsNullOrEmpty(userInput) ? defaultUser : userInput;
            // get the password
            Console.Write($"Enter Password (default: {defaultPassword}): ");
            string passwordInput = Console.ReadLine() ?? defaultPassword;
            string password = string.IsNullOrEmpty(passwordInput) ? defaultPassword : passwordInput;
            // get the role
            Console.Write($"Enter Role (default: {defaultRole}): ");
            string roleInput = Console.ReadLine() ?? defaultRole;
            string role = string.IsNullOrEmpty(roleInput) ? defaultRole : roleInput;
            // Try connecting to the SEP Instance
            try{
                // Build the Connection Properties.
                TrinoConnectionProperties SEPProperties = new TrinoConnectionProperties()
                {
                    AllowHostNameCNMismatch = true,
                    AllowSelfSignedServerCert = true,
                    Auth = new BasicAuth{
                        User = user,
                        Password = password
                    },
                    Catalog = "tpcds",
                    Server = new Uri(sepUrl),
                    EnableSsl = true,
                    Source = "SEPonADONET",
                    User = user,
                    // Role to be tested
                    Roles = new Dictionary<string, ClientSelectedRole>()
                    {
                        { "system", new ClientSelectedRole(ClientSelectedRole.Type.NONE,role) }
                    }
                };
                // Open a connection to the Trino Server
                TrinoConnection sep = new TrinoConnection(SEPProperties);
                IDbCommand trinoQuery = new TrinoCommand(sep, "select c_customer_sk,c_birth_country from tpcds.tiny.customer");
                IDataReader sepResults = trinoQuery.ExecuteReader();
                // Print column headers
                for (int i = 0; i < sepResults.FieldCount; i++)
                {
                    Console.Write(sepResults.GetName(i).PadRight(15));
                }
                Console.WriteLine();

                // Print separator
                Console.WriteLine(new string('-', sepResults.FieldCount * 15));

                // Print data rows
                while (sepResults.Read())
                {
                    for (int i = 0; i < sepResults.FieldCount; i++)
                    {
                        // Handle Nulls.
                        if (sepResults[i] != null)
                        {
                            Console.Write((sepResults[i]?.ToString() ?? "NULL").PadRight(15));
                        } else
                        {
                            Console.Write("NULL");
                        }
                        
                    }
                    Console.WriteLine();
                }
                sepResults.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed connect to {sepUrl} as {user} with {role}: {e.Message}");
            }
        }
    }
}