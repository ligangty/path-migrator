package cmd

import "github.com/spf13/cobra"

var RootCmd = &cobra.Command{
	Use:   "indy-pathmap-migrator",
	Short: "Pathmap migrate ",
	Long:  `Pathmap migrate`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func init() {
	RootCmd.AddCommand(scanCmd)
}
