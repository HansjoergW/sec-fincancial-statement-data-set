"""Contains the definitions to standardize incaome statements."""
from typing import List, Set

import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.base_prepivot_rules import PrePivotDeduplicate, PrePivotCorrectSign
from secfsdstools.f_standardize.base_rule_framework import RuleGroup, Rule
from secfsdstools.f_standardize.base_rules import CopyTagRule, SumUpRule, SubtractFromRule, \
    missingsumparts_rules_creator, MissingSummandRule, PostSetToZero, PostFixSign
from secfsdstools.f_standardize.base_validation_rules import ValidationRule, SumValidationRule
from secfsdstools.f_standardize.standardizing import Standardizer

# The 100 most common tags which appear between GrossProfit and OperatingIncomeLoss
top_100_operating_costs = [
    'SellingGeneralAndAdministrativeExpense',
    'GeneralAndAdministrativeExpense',
    'ResearchAndDevelopmentExpense',
    'SellingAndMarketingExpense',
    'AmortizationOfIntangibleAssets',
    'DepreciationAndAmortization',
    'RestructuringCharges',
    'ProfessionalFees',
    'SellingExpense',
    'AssetImpairmentCharges',
    'OtherOperatingIncomeExpenseNet',
    'DepreciationDepletionAndAmortization',
    'LaborAndRelatedExpense',
    'BusinessCombinationAcquisitionRelatedCosts',
    'GoodwillImpairmentLoss',
    'Depreciation',
    'OtherCostAndExpenseOperating',
    'MarketingAndAdvertisingExpense',
    'RestructuringSettlementAndImpairmentProvisions',
    'SalariesAndWages',
    'ShareBasedCompensation',
    'ProvisionForDoubtfulAccounts',
    'GainLossOnDispositionOfAssets',
    'CostsAndExpenses',
    'OperatingCostsAndExpenses',
    'OtherGeneralAndAdministrativeExpense',
    'ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost',
    'GainLossOnSaleOfPropertyPlantEquipment',
    'GoodwillAndIntangibleAssetImpairment',
    'MarketingExpense',
    'GainLossOnDispositionOfAssets1',
    'AdvertisingExpense',
    'BusinessCombinationContingentConsiderationArrangementsChangeInAmountOfContingentConsiderationLiability1',
    'LeaseAndRentalExpense',
    'LegalFees',
    'OtherSellingGeneralAndAdministrativeExpense',
    'GainLossRelatedToLitigationSettlement',
    'IncomeLossFromEquityMethodInvestments',
    'DepreciationNonproduction',
    'AllocatedShareBasedCompensationExpense',
    'SalariesWagesAndOfficersCompensation',
    'TravelAndEntertainmentExpense',
    'ImpairmentOfLongLivedAssetsHeldForUse',
    'ResearchAndDevelopmentExpenseSoftwareExcludingAcquiredInProcessCost',
    'OfficersCompensation',
    'OtherGeneralExpense',
    'ImpairmentOfIntangibleAssetsExcludingGoodwill',
    'OtherOperatingIncome',
    'ProfessionalAndContractServicesExpense',
    'LitigationSettlementExpense',
    'GainLossOnSaleOfBusiness',
    'ImpairmentOfIntangibleAssetsFinitelived',
    'RestructuringCostsAndAssetImpairmentCharges',
    'ForeignCurrencyTransactionGainLossBeforeTax',
    'OtherDepreciationAndAmortization',
    'OtherExpenses',
    'RestructuringCosts',
    'SalesCommissionsAndFees',
    'BusinessCombinationIntegrationRelatedCosts',
    'AdjustmentForAmortization',
    'InterestExpense',
    'ImpairmentOfIntangibleAssetsIndefinitelivedExcludingGoodwill',
    'CommunicationsAndInformationTechnology',
    'PreOpeningCosts',
    'OtherNonrecurringIncomeExpense',
    'GainLossOnSalesOfAssetsAndAssetImpairmentCharges',
    'OtherAssetImpairmentCharges',
    'ImpairmentOfLongLivedAssetsToBeDisposedOf',
    'DisposalGroupNotDiscontinuedOperationGainLossOnDisposal',
    'GainLossOnSaleOfOtherAssets',
    'ShippingHandlingAndTransportationCosts',
    'RoyaltyExpense',
    'GeneralInsuranceExpense',
    'ManagementFeeExpense',
    'GainsLossesOnSalesOfAssets',
    'OccupancyNet',
    'TaxesExcludingIncomeAndExciseTaxes',
    'TechnologyServicesCosts',
    'OtherIncome',
    'RestructuringAndRelatedCostIncurredCost',
    'OtherLaborRelatedExpenses',
    'SeveranceCosts1',
    'UtilitiesOperatingExpenseMaintenanceAndOperations',
    'EmployeeBenefitsAndShareBasedCompensation',
    'BusinessDevelopment',
    'CostsAndExpensesRelatedParty',
    'ExplorationExpenseMining',
    'DepreciationAmortizationAndAccretionNet',
    'GainLossOnDispositionOfProperty',
    'InventoryWriteDown',
    'AssetRetirementObligationAccretionExpense',
    'InsuranceRecoveries',
    'AmortizationOfAcquiredIntangibleAssets',
    'GainsLossesOnExtinguishmentOfDebt',
    'AmortizationOfAcquisitionCosts',
    'TangibleAssetImpairmentCharges',
    'ResearchAndDevelopmentInProcess',
    'OperatingLeasesRentExpenseNet',
    'OperatingLeaseExpense',
]


class PostFixIncomeLossFromContinuingOperationsAndSignOfIncomeTaxExpenseBenefit(Rule):
    """
    Usually, when TaxExpenseBenefit is a positive number, meaning that this amount
    of taxes were paid by the company. Therefore, the equation

    IncomeLossBeforeTaxes - TaxExpenseBenefit = NetIncomeLoss

    However, there  is a quite significant number of reports, which us a negative
    number to display paid Taxes and therefore, the following equations would be
    used:

    IncomeLossBeforeTaxes + TaxExpenseBenefit = NetIncomeLoss

    This rule select these entries and tries to fix that, whenever possible.

    Since the rules calculate IncomeLossFromContinuingOperations as follows

    IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit
    - IncomeTaxExpenseBenefit
    = IncomeLossFromContinuingOperations

    We might end up having a wrong value for IncomeLossFromContinuingOperation.

    Therefore, we mask the entries, where

    abs(NetIncomeLoss - IncomeLossFromContinuingOperations) = 2*abs(IncomeTaxExpenseBenefit)
    if IncomeTaxExpenseBenefit > 0

    in order to fix the sign for IncomeTaxExpenseBenefit and to correct
    IncomeLossFromContinuingOperations

    The rule also uses some threshold.

    Of course, this is not perfect and does not consider the cases with DiscontinuedOperations,
    However, it is able to fix a significant number of cases.

    For the masked entries, it changes the sign of TaxExpenseBenefit
    and recalculates IncomeLossFromContinuingOperations

    """

    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns
        """
        return ['IncomeLossFromContinuingOperations', 'AllIncomeTaxExpenseBenefit']

    def get_input_tags(self) -> Set[str]:
        """
        return all tags that the rules within this group need.

        Returns:
            Set[str]: set of all input tags that are used by rules in this group
        """
        return {'IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
                'AllIncomeTaxExpenseBenefit', 'IncomeLossFromContinuingOperations', 'ProfitLoss'}

    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            data_df dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        return (data_df.AllIncomeTaxExpenseBenefit != 0) & \
            ((data_df.ProfitLoss - data_df.IncomeLossFromContinuingOperations).abs() >=
             1.8 * data_df.AllIncomeTaxExpenseBenefit.abs())

    def apply(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """

        data_df.loc[mask, 'AllIncomeTaxExpenseBenefit'] = -data_df.AllIncomeTaxExpenseBenefit

        # recalculate IncomeLossFromContinuingOperations
        data_df.loc[mask, 'IncomeLossFromContinuingOperations'] = \
            data_df['IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit']
        subtract_mask = mask & ~data_df.AllIncomeTaxExpenseBenefit.isna()
        data_df.loc[subtract_mask, 'IncomeLossFromContinuingOperations'] = (
                data_df['IncomeLossFromContinuingOperations'] -
                data_df.AllIncomeTaxExpenseBenefit)

    def get_description(self) -> str:
        return "Corrects the sign of 'IncomeTaxExpenseBenefits' if it seems to be wrong. Checks" \
               "if the difference between 'NetIncomeLoss' and 'IncomeLossFromContinuingOperations'" \
               "is about 2. Which is a clear indication, that the sign was wrong."


class IncomeStatementStandardizer(Standardizer):
    """
    The goal of this Standardizer is to create IncomeStatements that are comparable,
    meaning that they have the same tags.

    At the end, the standardized IS contains the following columns

    Position	Relation

    Revenues
           + SalesRevenueGoodsNet
           + SalesRevenueServicesNet
           + OtherSalesRevenueNet
           --------------
        or SalesRevenueNet

        or RevenuesSum
              RevenueFromContractWithCustomerExcludingAssessedTax
              RevenueFromContractWithCustomerIncludingAssessedTax
              RevenuesExcludingInterestAndDividends
              RegulatedAndUnregulatedOperatingRevenue
              ...

        or InterestAndDividendIncomeOperating
        --------
        Revenues
        ========

    CostOfRevenue
          'CostOfGoodsSold',
        + 'CostOfServices'
        ---------------
          'CostOfGoodsAndServicesSold',
        + 'LicenseCost',
        ----------------
          'CostOfRevenue',
        ================


    Gross Profit	                Revenues - Cost of Goods and Services Sold
    Operating Expenses

    Operating Income (Loss)	        Gross Profit - Operating Expenses
    Other Income and Expenses

    Income Before Tax               Operating Income + Other Income and Expenses
    Income Tax Expense (Benefit)

    Net Income (Loss)               Income Before Tax - Income Tax Expense
    """
    prepivot_rule_tree = RuleGroup(
        prefix="IS_PREPIV",
        rules=[PrePivotDeduplicate(),
               PrePivotCorrectSign(
                   # all these tags are costOf and therefore should appear as a positive number
                   # since costs are subtracted from the Revenue to get the GrossProfit
                   tag_list=['CostOfGoodsSold',
                             'CostOfServices',
                             'CostOfGoodsAndServicesSold',
                             'CostOfRevenue',
                             'CostOfGoodsSoldExcludingDepreciationDepletionAndAmortization',
                             'CostOfGoodsSoldDepreciationDepletionAndAmortization',
                             'CostOfGoodsSoldDepletion',
                             'CostOfGoodsSoldDepreciation',
                             'CostOfGoodsSoldAmortization',
                             'CostOfGoodsSoldDepreciationAndAmortization',
                             'CostOfGoodsSoldDirectFinancingLease',
                             'CostOfGoodsSoldElectric',
                             'CostOfGoodsSoldDirectMaterials',
                             'CostOfGoodsSoldDirectLabor',
                             'CostOfGoodsSoldOverhead',
                             'CostOfGoodsSoldOilAndGas',
                             'CostOfGoodsSoldSubscription',
                             'CostOfGoodsSoldDirectTaxesAndLicensesCosts',
                             'CostOfGoodsSoldMaintenanceCosts',
                             'CostOfGoodsSoldSalesTypeLease',
                             'CostOfServicesExcludingDepreciationDepletionAndAmortization',
                             'CostOfServicesDepreciation',
                             'CostOfServicesDepreciationAndAmortization',
                             'CostOfServicesCatering',
                             'CostOfServicesAmortization',
                             'CostOfServicesOilAndGas',
                             'CostOfServicesDirectTaxesAndLicensesCosts',
                             'CostOfServicesMaintenanceCosts',
                             'CostOfServicesLicensesAndServices',
                             'CostOfServicesDirectLabor',
                             'CostOfServicesLicensesAndMaintenanceAgreements',
                             'CostOfServicesEnergyServices',
                             'CostOfServicesDirectMaterials',
                             'CostOfServicesEnvironmentalRemediation',
                             'CostOfServicesOverhead',
                             'CostOfGoodsAndServicesSoldDepreciationAndAmortization',
                             'CostOfGoodsAndServicesSoldAmortization',
                             'CostOfGoodsAndServicesSoldOverhead',
                             'CostOfGoodsAndServicesSoldDepreciation',
                             'CostOfGoodsAndServicesEnergyCommoditiesAndServices',
                             'CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization'
                             ],
                   is_positive=True
               )]
    )

    is_revenue_rg = RuleGroup(
        prefix="Rev",
        rules=[
            # normally, (Goods and Services) Gross and Net appear together, resp. Net has priority.
            # so if net is not present, we copy the gross value in the net value
            # and then sum the nets up to the SalesRevenue
            CopyTagRule(
                original='SalesRevenueGoodsGross',
                target='SalesRevenueGoodsNet'),

            CopyTagRule(
                original='SalesRevenueServicesGross',
                target='SalesRevenueServicesNet'),

            SumUpRule(sum_tag='SalesRevenueNet',
                      potential_summands=[
                          'SalesRevenueGoodsNet',
                          'SalesRevenueServicesNet'],
                      optional_summands=[
                          'OtherSalesRevenueNet'
                      ]),
            # if the Revenues is not set, we copy SalesRevenuesNet into Revenues
            CopyTagRule(original='SalesRevenueNet', target='Revenues'),

            # RevenueFromContractWithCustomer-tags are also often used to report the
            # total Revenue. so the following rules take care of these cases.
            # the order of the rules also defines the precedence.
            # first: if RevenueFromContractWithCustomerExcludingAssessedTax is not set
            #    we calculate it from RevenueFromContractWithCustomerIncludingAssessedTax
            #          and ExciseAndSalesTaxes (if present)
            SubtractFromRule(
                subtract_from_tag='RevenueFromContractWithCustomerIncludingAssessedTax',
                potential_subtract_tags=['ExciseAndSalesTaxes'],
                target_tag='RevenueFromContractWithCustomerExcludingAssessedTax'),

            # second Excluding has the higher precedence than Including
            CopyTagRule(original='RevenueFromContractWithCustomerExcludingAssessedTax',
                        target='Revenues'),
            CopyTagRule(original='RevenueFromContractWithCustomerIncludingAssessedTax',
                        target='Revenues'),

            # if Revenues couldn't be defined so far, RevenuesExcludingInterestAndDividends
            # is also a tag that is commonly used to define the total Revenue
            CopyTagRule(original='RevenuesExcludingInterestAndDividends',
                        target='Revenues'),

            # if we were not able to define the Revenue so far, there are
            # a couple of other tags which define Revenue, so we count them together
            # and set the total as Revenue
            SumUpRule(sum_tag='RevenuesSum',
                      potential_summands=[
                          'RegulatedAndUnregulatedOperatingRevenue',
                          'HealthCareOrganizationPatientServiceRevenue',
                          'ContractsRevenue',
                          'RevenueOilAndGasServices',
                          'HealthCareOrganizationRevenue',
                          'RevenueMineralSales',
                          'SalesRevenueEnergyServices',
                          'RealEstateRevenueNet',
                          'OperatingLeasesIncomeStatementLeaseRevenue',
                          'LicensesRevenue', 'RevenueFromRelatedParties',
                          'BrokerageCommissionsRevenue', 'RoyaltyRevenue', 'OilAndGasSalesRevenue',
                          'OilAndGasRevenue', 'OtherRealEstateRevenue',
                          'TechnologyServicesRevenue', 'ManagementFeesRevenue',
                          'ReimbursementRevenue',
                          'OperatingLeasesIncomeStatementMinimumLeaseRevenue',
                          'FoodAndBeverageRevenue', 'MaintenanceRevenue',
                          'LicenseAndServicesRevenue', 'FranchiseRevenue', 'SubscriptionRevenue',
                          'FinancialServicesRevenue',
                          'RevenueFromGrants',
                          'GasGatheringTransportationMarketingAndProcessingRevenue',
                          'OccupancyRevenue', 'NaturalGasProductionRevenue',
                          'SalesRevenueServicesGross', 'InvestmentBankingRevenue',
                          'AdvertisingRevenue', 'RevenueOtherFinancialServices',
                          'OilAndCondensateRevenue', 'RevenueFromLeasedAndOwnedHotels',
                          'RevenuesNetOfInterestExpense', 'RegulatedAndUnregulatedOperatingRevenue',
                          'UnregulatedOperatingRevenue', 'ElectricUtilityRevenue',
                          'CargoAndFreightRevenue', 'OtherHotelOperatingRevenue',
                          'CasinoRevenue', 'RefiningAndMarketingRevenue',
                          'PrincipalTransactionsRevenue', 'InterestRevenueExpenseNet',
                          'HomeBuildingRevenue', 'OtherRevenueExpenseFromRealEstateOperations',
                          'GasDomesticRegulatedRevenue', 'LicenseAndMaintenanceRevenue',
                          'RegulatedOperatingRevenue', 'AdmissionsRevenue', 'PassengerRevenue'
                      ],
                      optional_summands=[
                          'OtherSalesRevenueNet'
                      ]),

            CopyTagRule(original='RevenuesSum', target='Revenues'),
            # if there is nothing else
            CopyTagRule(original='InvestmentIncomeInterest', target='Revenues')
        ]
    )

    is_costofrevenue_rg = RuleGroup(
        prefix="costrevenue",
        rules=[
            SumUpRule(sum_tag='CostOfGoodsSold',
                      potential_summands=[
                          'CostOfGoodsSoldExcludingDepreciationDepletionAndAmortization',
                          'CostOfGoodsSoldDepreciationDepletionAndAmortization',
                          'CostOfGoodsSoldDepletion',
                          'CostOfGoodsSoldDepreciation',
                          'CostOfGoodsSoldAmortization',
                          'CostOfGoodsSoldDepreciationAndAmortization',
                          'CostOfGoodsSoldDirectFinancingLease',
                          'CostOfGoodsSoldElectric',
                          'CostOfGoodsSoldDirectMaterials',
                          'CostOfGoodsSoldDirectLabor',
                          'CostOfGoodsSoldOverhead',
                          'CostOfGoodsSoldOilAndGas',
                          'CostOfGoodsSoldSubscription',
                          'CostOfGoodsSoldDirectTaxesAndLicensesCosts',
                          'CostOfGoodsSoldMaintenanceCosts',
                          'CostOfGoodsSoldSalesTypeLease']),

            SumUpRule(sum_tag='CostOfServices',
                      potential_summands=[
                          'CostOfServicesExcludingDepreciationDepletionAndAmortization',
                          'CostOfServicesDepreciation',
                          'CostOfServicesDepreciationAndAmortization',
                          'CostOfServicesCatering',
                          'CostOfServicesAmortization',
                          'CostOfServicesOilAndGas',
                          'CostOfServicesDirectTaxesAndLicensesCosts',
                          'CostOfServicesMaintenanceCosts',
                          'CostOfServicesLicensesAndServices',
                          'CostOfServicesDirectLabor',
                          'CostOfServicesLicensesAndMaintenanceAgreements',
                          'CostOfServicesEnergyServices',
                          'CostOfServicesDirectMaterials',
                          'CostOfServicesEnvironmentalRemediation',
                          'CostOfServicesOverhead'
                      ]),

            SumUpRule(sum_tag='CostOfGoodsAndServicesSold',
                      potential_summands=[
                          'CostOfGoodsSold',
                          'CostOfServices']),

            SumUpRule(sum_tag='CostOfGoodsAndServicesSold',
                      potential_summands=[
                          'CostOfGoodsAndServicesSoldDepreciationAndAmortization',
                          'CostOfGoodsAndServicesSoldAmortization',
                          'CostOfGoodsAndServicesSoldOverhead',
                          'CostOfGoodsAndServicesSoldDepreciation',
                          'CostOfGoodsAndServicesEnergyCommoditiesAndServices',
                          'CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization']
                      ),
            CopyTagRule(original='CostOfGoodsAndServicesSold', target='CostOfRevenue'),
        ]
    )

    # GrossProfit is the only tag used to indicate GrossProfit

    is_missing_rev_cost_gross = RuleGroup(
        prefix="RevCostGross",
        rules=missingsumparts_rules_creator(sum_tag='Revenues',
                                            summand_tags=['CostOfRevenue', 'GrossProfit'])
    )

    is_grossProfit = RuleGroup(
        prefix='grossprofit',
        rules=[
            # for investment companies
            CopyTagRule(original='GrossInvestmentIncomeOperating', target='GrossProfit'),
            CopyTagRule(original='InterestAndDividendIncomeOperating', target='GrossProfit')
        ]
    )

    is_operating = RuleGroup(
        prefix="operating",
        rules=[
            RuleGroup(
                prefix="GrossOperating",
                rules=missingsumparts_rules_creator(sum_tag='GrossProfit',
                                                    summand_tags=['OperatingExpenses',
                                                                  'OperatingIncomeLoss'])),
            SumUpRule(sum_tag='OperatingExpensesSum',
                      potential_summands=top_100_operating_costs),
            CopyTagRule(original='OperatingExpensesSum', target='OperatingExpenses')
        ])

    is_netincome_rg = RuleGroup(
        prefix="netincome",
        rules=[
            # Renaming
            CopyTagRule(
                original='IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
                target='IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit'),

            SumUpRule(
                sum_tag='IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
                potential_summands=[
                    'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
                ], optional_summands=['IncomeLossFromEquityMethodInvestments']),

            CopyTagRule(
                original='IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest',
                target='IncomeLossFromContinuingOperations'),

            SumUpRule(
                sum_tag='AllIncomeTaxExpenseBenefit',
                potential_summands=[
                    'IncomeTaxExpenseBenefit', 'DeferredIncomeTaxExpenseBenefit'
                ], optional_summands=['IncomeLossFromEquityMethodInvestments']),

            SubtractFromRule(
                target_tag='IncomeLossFromContinuingOperations',
                subtract_from_tag='IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
                potential_subtract_tags=['AllIncomeTaxExpenseBenefit']
            ),

            CopyTagRule(
                original='IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity',
                target='IncomeLossFromDiscontinuedOperationsNetOfTax'),

            SumUpRule(sum_tag='ProfitLossParts',
                      potential_summands=[
                          'IncomeLossFromContinuingOperations',
                          'IncomeLossFromDiscontinuedOperationsNetOfTax'
                      ]),

            CopyTagRule(original='NetIncomeLossParts', target='ProfitLoss'),
            # if there is no value for ProfitLoss, we set it to NetIncomeLoss
            CopyTagRule(original='NetIncomeLoss', target='ProfitLoss'),

            # since IncomeLossFromContinuingOperations is part of a validation rule,
            # and if it was not set, we assume that there is only IncomeLossFromContinuingOperations
            # copy the value from ProfitLoss
            CopyTagRule(original='ProfitLoss', target='IncomeLossFromContinuingOperations'),

            # the following rules set the NetIncomeLoss, if not already set.
            # the order of the rules is the precedence
            RuleGroup(prefix="fill_NIL",
                      rules=[
                          CopyTagRule(original='NetIncomeLossAvailableToCommonStockholdersBasic',
                                      target='NetIncomeLoss'),
                          CopyTagRule(original='NetIncomeLossAllocatedToLimitedPartners',
                                      target='NetIncomeLoss'),
                          CopyTagRule(original='ProfitLoss', target='NetIncomeLoss'),
                          CopyTagRule(
                              original='ProfitLossIncludingRedeemableNonControllingInterest',
                              target='NetIncomeLoss'),
                          CopyTagRule(original='OtherComprehensiveIncomeLossNetOfTax',
                                      target='NetIncomeLoss'),
                          CopyTagRule(original='ComprehensiveIncomeNetOfTax',
                                      target='NetIncomeLoss'),

                          # ***** NEW
                          CopyTagRule(original='IncomeLossAttributableToParent',
                                      target='NetIncomeLoss'),

                          # for investment companies
                          CopyTagRule(original='NetInvestmentIncome', target='NetIncomeLoss'),
                      ])
        ]
    )

    is_missing_NTI_PL = RuleGroup(
        prefix="MissingNTI_PL",
        rules=missingsumparts_rules_creator(
            sum_tag='IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
            summand_tags=['IncomeLossFromContinuingOperations',
                          'AllIncomeTaxExpenseBenefit']) +
              [
                  MissingSummandRule(sum_tag='ProfitLoss',
                                     existing_summands_tags=['NetIncomeLoss'],
                                     missing_summand_tag='NetIncomeLossAttributableToNoncontrollingInterest'
                                     )
              ]
    )

    main_rule_tree = RuleGroup(prefix="IS",
                               rules=[
                                   is_revenue_rg,
                                   is_costofrevenue_rg,
                                   is_missing_rev_cost_gross,
                                   is_grossProfit,
                                   is_operating,
                                   is_netincome_rg,
                                   is_missing_NTI_PL
                               ])

    preprocess_rule_tree = RuleGroup(prefix="IS_PRE",
                                     rules=[
                                     ])

    post_rule_tree = RuleGroup(
        prefix="IS_POST",
        rules=[
            PostFixIncomeLossFromContinuingOperationsAndSignOfIncomeTaxExpenseBenefit(),
            PostFixSign(start_tag='ProfitLoss',
                        summand_tag='AllIncomeTaxExpenseBenefit',
                        result_tag='IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit'),
            PostFixSign(start_tag='NetIncomeLoss',
                        summand_tag='NetIncomeLossAttributableToNoncontrollingInterest',
                        result_tag='ProfitLoss'),

            # hier noch 2. Rule machen, die nur IncomeTaxExpenseBenefit korrigiert
            #
            # benötigt eine Regel, welche nur IncomeTaxExpenseBenefit korrigiert,
            # falls BeforeTax - AfterTex == 2*incomeTaxExpenseBenefit,
            # bzw. falls BeforeTax + IncomeTaxExpenseBenefit == After Tax

            # 1. if there is no value for Revenues, but GrossProfit
            # we set Revenues to GrossProfit
            # 2. if there is no value for  GrossProfit, but Revenues
            # we set GrossProfit to Revenues
            # 3. in both cases, CostOfRevenue is set to zero with the following
            # MissingSummandRule.
            CopyTagRule(original='GrossProfit', target='Revenues'),
            CopyTagRule(original='Revenues', target='GrossProfit'),
            MissingSummandRule(sum_tag='Revenues',
                               existing_summands_tags=['GrossProfit'],
                               missing_summand_tag='CostOfRevenue'),
            # Next, we finalize OperatingIncomeLoss, by subtracting
            # a last time the OperatingExpenses from the GrossProfit,
            # if OperatingIncomeLoss is not set
            SubtractFromRule(subtract_from_tag='GrossProfit',
                             potential_subtract_tags=['OperatingExpenses'],
                             target_tag='OperatingIncomeLoss'),

            # 1. if there is no value for IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit,
            # but for ProfitLoss, we set it to the same value
            # 2. afterward, we set IncomeTaxExpenseBenefit to zero with the following rule
            CopyTagRule(original='ProfitLoss',
                        target='IncomeLossFromContinuingOperations'),
            CopyTagRule(original='IncomeLossFromContinuingOperations',
                        target='IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit'),

            MissingSummandRule(
                sum_tag='IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
                existing_summands_tags=['IncomeLossFromContinuingOperations'],
                missing_summand_tag='AllIncomeTaxExpenseBenefit'),

            PostSetToZero(tags=['IncomeLossFromDiscontinuedOperationsNetOfTax']),

            # if IncomeLossFromContinuingOperations is not set, but IL..BeforeIncomeTaxExpenseBenefit is set
            # set it to IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit
            CopyTagRule(original='IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
                        target='IncomeLossFromContinuingOperations'),

            # since new IncomeLossFromContinuingOperations values could have been set,
            # calculate missing ProfitLoss entries
            SumUpRule(sum_tag='ProfitLoss',
                      potential_summands=[
                          'IncomeLossFromContinuingOperations',
                          'IncomeLossFromDiscontinuedOperationsNetOfTax'
                      ]),

            # since new ProfitLoss could have been set, use it to set still missing NetIncomeLoss
            CopyTagRule(original='ProfitLoss',
                        target='NetIncomeLoss'),

            # calculate missing NetIncomeLossAttributableToNoncontrollingInterest
            MissingSummandRule(
                sum_tag='ProfitLoss',
                existing_summands_tags=['NetIncomeLoss'],
                missing_summand_tag='NetIncomeLossAttributableToNoncontrollingInterest'),


        ])

    validation_rules: List[ValidationRule] = [
        SumValidationRule(identifier="RevCogGrossCheck",
                          sum_tag='Revenues',
                          summands=['CostOfRevenue', 'GrossProfit']),
        SumValidationRule(identifier="GrossCstopOpinc",
                          sum_tag='GrossProfit',
                          summands=['OperatingExpenses', 'OperatingIncomeLoss']),
        SumValidationRule(identifier="ContIncTax",
                          sum_tag='IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
                          summands=['AllIncomeTaxExpenseBenefit',
                                    'IncomeLossFromContinuingOperations']),
        SumValidationRule(identifier='ProfitLoss',
                          sum_tag='ProfitLoss',
                          summands=[
                              'IncomeLossFromContinuingOperations',
                              'IncomeLossFromDiscontinuedOperationsNetOfTax'
                          ]),
        SumValidationRule(identifier='NetIncomeLoss',
                          sum_tag='ProfitLoss',
                          summands=[
                              'NetIncomeLoss',
                              'NetIncomeLossAttributableToNoncontrollingInterest'
                          ]),

    ]

    # these are the columns that finally are returned after the standardization
    final_tags: List[str] = ['Revenues',
                             'CostOfRevenue',
                             'GrossProfit',
                             'OperatingExpenses',
                             'OperatingIncomeLoss',
                             'IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
                             'AllIncomeTaxExpenseBenefit',
                             'IncomeLossFromContinuingOperations',
                             'IncomeLossFromDiscontinuedOperationsNetOfTax',
                             'ProfitLoss',
                             'NetIncomeLossAttributableToNoncontrollingInterest',
                             'NetIncomeLoss'
                             ]

    # used to evaluate if a report is the main balancesheet report
    # inside a report, there can be several tables (different report nr)
    # which stmt value is BS.
    # however, we might be only interested in the "major" BS report. Usually this is the
    # one which has the least nan in the following columns
    main_statement_tags = ['Revenues',
                           'SalesRevenueNet',
                           'SalesRevenueGoodsNet',
                           'SalesRevenueServicesNet',
                           'CostOfRevenue',
                           'CostOfGoodsAndServicesSold',
                           'CostOfGoodsSold',
                           'CostOfServices',
                           'GrossProfit',
                           'OperatingExpenses',
                           'OperatingIncomeLoss',
                           'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
                           'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
                           'IncomeLossBeforeIncomeTaxExpenseBenefit',
                           'IncomeTaxExpenseBenefit',
                           'DeferredIncomeTaxExpenseBenefit',
                           'NetIncomeLoss',
                           'ProfitLoss'
                           ]

    def __init__(self, filter_for_main_statement: bool = True, iterations: int = 3):
        super().__init__(
            prepivot_rule_tree=self.prepivot_rule_tree,
            pre_rule_tree=self.preprocess_rule_tree,
            main_rule_tree=self.main_rule_tree,
            post_rule_tree=self.post_rule_tree,
            validation_rules=self.validation_rules,
            final_tags=self.final_tags,
            main_iterations=iterations,
            filter_for_main_statement=filter_for_main_statement,
            main_statement_tags=self.main_statement_tags)
