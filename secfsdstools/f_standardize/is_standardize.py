"""Contains the definitions to standardize incaome statements."""
from typing import List

from secfsdstools.f_standardize.base_prepivot_rules import PrePivotDeduplicate, PrePivotCorrectSign
from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.base_rules import CopyTagRule, SumUpRule, SubtractFromRule, \
    missingsumparts_rules_creator, MissingSummandRule
from secfsdstools.f_standardize.base_validation_rules import ValidationRule
from secfsdstools.f_standardize.standardizing import Standardizer

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
                   tag_list=['CostOfGoodsSold', 'CostOfServices',
                             'CostOfGoodsAndServicesSold', 'LicenseCost',
                             'CostOfRevenue'],
                   is_positive=True
               )]
    )

    is_cogs_sumup_rg = RuleGroup(
        prefix="IS_COGSUMUP",
        rules=[
            SumUpRule(sum_tag='CostOfGoodsAndServicesSold',
                      potential_summands=['CostOfGoodsSold', 'CostOfServices']),
            SumUpRule(sum_tag='CostOfRevenue',
                      potential_summands=['CostOfGoodsAndServicesSold', 'LicenseCost'])
        ]
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
            SumUpRule(sum_tag='OperatingExpensesSum',
                      potential_summands=top_100_operating_costs),
            RuleGroup(
                prefix="GrossOperating",
                rules=missingsumparts_rules_creator(sum_tag='GrossProfit',
                                                    summand_tags=['OperatingExpenses',
                                                                  'OperatingIncomeLoss'])),
            CopyTagRule(original='OperatingExpensesSum', target='OperatingExpenses')
        ])

    is_netincome_rg = RuleGroup(
        prefix="netincome",
        rules=[
            SumUpRule(
                sum_tag='IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
                potential_summands=[
                    'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
                    'IncomeLossFromEquityMethodInvestments'
                ]),

            SubtractFromRule(
                target_tag='IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest',
                subtract_from_tag='IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
                potential_subtract_tags=['IncomeTaxExpenseBenefit']
                ),

            SumUpRule(sum_tag='NetIncomeLossParts',
                      potential_summands=[
                          'IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest',
                          'IncomeLossFromDiscontinuedOperationsNetOfTax'
                      ]),

            # Renaming
            CopyTagRule(
                original='IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
                target='IncomeLossBeforeIncomeTaxExpenseBenefit'),

            # the following rules set the NetIncomeLoss, if not already set.
            # the order of the rules is the precedence
            CopyTagRule(original='NetIncomeLossAvailableToCommonStockholdersBasic',
                        target='NetIncomeLoss'),
            CopyTagRule(original='NetIncomeLossAllocatedToLimitedPartners', target='NetIncomeLoss'),
            CopyTagRule(original='ProfitLoss', target='NetIncomeLoss'),
            CopyTagRule(original='OtherComprehensiveIncomeLossNetOfTax', target='NetIncomeLoss'),
            CopyTagRule(original='ComprehensiveIncomeNetOfTax', target='NetIncomeLoss'),
            CopyTagRule(original='NetIncomeLossParts', target='NetIncomeLoss'),
            # for investment companies
            CopyTagRule(original='NetInvestmentIncome', target='NetIncomeLoss'),
        ]
    )

    is_missing_net_tax_income = RuleGroup(
        prefix="NetTaxIncome",
        rules=missingsumparts_rules_creator(
            sum_tag='NetIncomeLoss',
            summand_tags=['IncomeLossBeforeIncomeTaxExpenseBenefit',
                          'IncomeTaxExpenseBenefit'])
    )



    main_rule_tree = RuleGroup(prefix="IS",
                               rules=[
                                   is_revenue_rg,
                                   is_costofrevenue_rg,
                                   is_missing_rev_cost_gross,
                                   is_grossProfit,
                                   is_operating,
                                   is_netincome_rg,
                                   is_missing_net_tax_income
                               ])

    preprocess_rule_tree = RuleGroup(prefix="IS_PRE",
                                     rules=[
                                     ])

    post_rule_tree = RuleGroup(prefix="IS_POST",
                               rules=[
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
                                                    target_tag='OperatingIncomeLoss')
                               ])

    validation_rules: List[ValidationRule] = [
    ]

    # these are the columns that finally are returned after the standardization
    final_tags: List[str] = ['Revenues',
                             'CostOfRevenue',
                             'GrossProfit',
                             'OperatingExpenses',
                             'OperatingIncomeLoss',
                             'IncomeLossBeforeIncomeTaxExpenseBenefit',
                             'IncomeTaxExpenseBenefit',
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
